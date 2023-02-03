#include <cmath>
#include <cstdint>
#include <fstream>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/tokenizer.hpp>
#include <curl/curl.h>

#include "baldr/graphconstants.h"
#include "baldr/graphid.h"
#include "baldr/graphtile.h"
#include "baldr/rapidjson_utils.h"
#include "baldr/tilehierarchy.h"
#include "midgard/encoded.h"
#include "midgard/logging.h"
#include "midgard/sequence.h"
#include "midgard/tiles.h"

#include "filesystem.h"
#include "just_gtfs/just_gtfs.h"
#include "midgard/util.h"
#include "mjolnir/admin.h"
#include "mjolnir/ingest_transit.h"
#include "mjolnir/servicedays.h"
#include "mjolnir/util.h"
#include "proto/transit.pb.h"

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

using namespace boost::property_tree;
using namespace valhalla::midgard;
using namespace valhalla::baldr;
using namespace valhalla::mjolnir;

namespace {

struct feedObject {
  gtfs::Id id;
  std::string feed;
};

} // namespace

// information referred by tileBuilder saved spatially
namespace std {
template <> struct hash<feedObject> {
  inline size_t operator()(const feedObject& o) const {
    return hash<string>()(o.id + o.feed);
  }
};
template <> struct equal_to<feedObject> {
  inline bool operator()(const feedObject& a, const feedObject& b) const {
    return a.id == b.id && a.feed == b.feed;
  }
};
} // namespace std

namespace {

struct tileTransitInfo {
  GraphId graphid;
  // TODO: unordered multimap-string to a pair of strings that maps from station (parent_id) ->
  //  platform/egress (child) (Max 2 kinds / many could exists)
  std::unordered_multimap<feedObject, gtfs::Id> station_children;
  std::unordered_set<feedObject> stations;
  std::unordered_set<feedObject> trips;
  std::unordered_map<feedObject, size_t> routes;
  std::unordered_set<feedObject> services;
  std::unordered_set<feedObject> agencies;
  std::unordered_map<feedObject, size_t> shapes;

  bool operator<(const tileTransitInfo& t1) const {
    // sort tileTransitInfo by size
    return stations.size() < t1.stations.size();
  }
};

struct feedCache {
  std::unordered_map<std::string, gtfs::Feed> cache;

  const gtfs::Feed& operator()(const feedObject& feed_object) {
    auto found = cache.find(feed_object.feed);
    if (found != cache.end()) {
      return found->second;
    }

    auto inserted = cache.insert({feed_object.feed, gtfs::Feed(feed_object.feed)});
    inserted.first->second.read_feed();
    return inserted.first->second;
  }
};

struct dist_sort_t {
  PointLL center;
  Tiles<PointLL> grid;
  dist_sort_t(const GraphId& center, const Tiles<PointLL>& grid) : grid(grid) {
    this->center = grid.TileBounds(center.tileid()).Center();
  }
  bool operator()(const GraphId& a, const GraphId& b) const {
    auto a_dist = center.Distance(grid.TileBounds(a.tileid()).Center());
    auto b_dist = center.Distance(grid.TileBounds(b.tileid()).Center());
    if (a_dist == b_dist) {
      return a.tileid() < b.tileid();
    }
    return a_dist < b_dist;
  }
};

struct unique_transit_t {
  std::mutex lock;
  std::unordered_map<std::string, size_t> trips;
  std::unordered_map<std::string, size_t> block_ids;
  std::unordered_map<std::string, size_t> lines;
};

// Read from GTFS feed, sort data into the tiles they belong to
std::priority_queue<tileTransitInfo> select_transit_tiles(const boost::property_tree::ptree& pt) {

  auto path = pt.get<std::string>("mjolnir.transit_feeds_dir");

  std::set<GraphId> tiles;
  const auto& local_tiles = TileHierarchy::levels().back().tiles;
  std::unordered_map<GraphId, tileTransitInfo> tile_map;

  // adds a tile_info for the tile_id to tile_map if there is none yet, and returns the tile_info
  // object
  auto get_tile_info = [&tile_map, &local_tiles](const gtfs::Stop& stop) -> tileTransitInfo& {
    tileTransitInfo tile_info;
    auto graphid = GraphId(local_tiles.TileId(stop.stop_lat, stop.stop_lon),
                           TileHierarchy::GetTransitLevel().level, 0);

    auto tile_inserted = tile_map.insert({graphid, tile_info});
    if (tile_inserted.second) {
      tile_inserted.first->second.graphid = graphid;
    }

    return tile_inserted.first->second;
  };

  filesystem::recursive_directory_iterator gtfs_feed_itr(path);
  filesystem::recursive_directory_iterator end_file_itr;
  for (; gtfs_feed_itr != end_file_itr; ++gtfs_feed_itr) {
    if (filesystem::is_directory(gtfs_feed_itr->path())) {
      auto feed_path = gtfs_feed_itr->path().string();
      LOG_INFO("Trying to load " + feed_path);
      gtfs::Feed feed(feed_path);
      feed.read_feed();
      LOG_INFO("Done Reading Feed");
      const auto& stops = feed.get_stops();

      // 1st pass to add all the stations, so we can add stops to its children in a 2nd pass
      for (const auto& stop : stops) {
        if (stop.location_type == gtfs::StopLocationType::Station) {
          auto& tile_info = get_tile_info(stop);
          tile_info.stations.insert({stop.stop_id, feed_path});
        }
      }

      // 2nd pass to add the platforms/stops
      for (const auto& stop : stops) {
        // TODO: GenericNode & BoardingArea could be useful at some point
        if (!(stop.location_type == gtfs::StopLocationType::StopOrPlatform) &&
            !(stop.location_type == gtfs::StopLocationType::EntranceExit)) {
          continue;
        }

        auto& tile_info = get_tile_info(stop);

        // if this station doesn't exist, we need to create it: we use the fact that this entry is
        // not a station type to fake a station object in write_stops()
        auto station_in_tile = tile_info.stations.find({stop.parent_station, feed_path});
        if (station_in_tile != tile_info.stations.end()) {
          tile_info.station_children.insert({{stop.parent_station, feed_path}, stop.stop_id});
        } else {
          // we don't have the parent station, if
          // 1) this stop has none or 2) its parent station is in another tile
          // TODO: need to handle the 2nd case somehow!
          tile_info.stations.insert({stop.stop_id, feed_path});
          tile_info.station_children.insert({{stop.stop_id, feed_path}, stop.stop_id});
        }

        for (const auto& stopTime : feed.get_stop_times_for_stop(stop.stop_id)) {
          // add trip, route, agency and service_id from stop_time, it's the only place with that info
          // TODO: should we throw here?
          auto trip = feed.get_trip(stopTime.trip_id);
          auto route = feed.get_route(trip->route_id);
          if (!trip || !route || trip->service_id.empty()) {
            LOG_ERROR("Missing trip or route or service_id for trip");
            continue;
          }

          tile_info.services.insert({trip->service_id, feed_path});
          tile_info.trips.insert({trip->trip_id, feed_path});
          tile_info.shapes.insert({{trip->shape_id, feed_path}, tile_info.shapes.size()});
          tile_info.routes.insert({{route->route_id, feed_path}, tile_info.routes.size()});
          tile_info.agencies.insert({route->agency_id, feed_path});
        }
      }

      LOG_INFO("Done parsing " + std::to_string(tile_map.size()) + " transit tiles for GTFS feed " +
               feed_path);
    }
  }
  std::priority_queue<tileTransitInfo> prioritized;
  for (auto it = tile_map.begin(); it != tile_map.end(); it++) {
    prioritized.push(it->second);
  }
  return prioritized;
}

void setup_stops(Transit& tile,
                 const gtfs::Stop& tile_stop,
                 GraphId& node_id,
                 std::unordered_map<feedObject, GraphId>& platform_node_ids,
                 const std::string& feed_path,
                 NodeType node_type,
                 bool isGenerated,
                 GraphId prev_id = {}) {
  auto* node = tile.mutable_nodes()->Add();
  node->set_lon(tile_stop.stop_lon);
  node->set_lat(tile_stop.stop_lat);

  // change set_type to match the child / parent type.
  node->set_type(static_cast<uint32_t>(node_type));
  node->set_graphid(node_id);

  if (node_type != NodeType::kTransitEgress) {
    node->set_prev_type_graphid(prev_id.Is_Valid() ? prev_id : node_id - 1);
  } // in/egresses need accessibility set so that transit connect edges inherit that access
  else {
    // TODO: its unclear how to determine unidirectionality (entrance-only vs exit-only) from the gtfs
    //  spec, perhaps one should use pathways.txt::is_bidirectional to differentiate?
    node->set_traversability(static_cast<uint32_t>(Traversability::kBoth));
  }
  node->set_name(tile_stop.stop_name);
  node->set_timezone(tile_stop.stop_timezone);
  bool wheelchair_accessible = (tile_stop.wheelchair_boarding == "1");
  node->set_wheelchair_boarding(wheelchair_accessible);
  node->set_generated(isGenerated);
  auto onestop_id = isGenerated ? tile_stop.stop_id + "_" + to_string(node_type) : tile_stop.stop_id;
  node->set_onestop_id(onestop_id);
  if (node_type == NodeType::kMultiUseTransitPlatform) {
    // when this platform is not generated, we can use its actual given id verbatim because thats how
    // other gtfs entities will refer to it. however when it is generated we must use its parents id
    // and not the generated one because the references in the feed have no idea that we are doing
    // the generation of new ideas for non-existant platforms
    platform_node_ids[{tile_stop.stop_id, feed_path}] = node_id;
  }
  node_id++;
}

/**
 * Writes all of the nodes of the graph in pbf tile
 * @param tile       the transit pbf tile to modify
 * @param tile_info  ephemeral info from the transit feeds which we write into the tile
 * @return a map of string gtfs id to graph id (so node id) for every platform in the tile
 *         later on we use these ids to connect platforms that reference each other in the schedule
 */
std::unordered_map<feedObject, GraphId> write_stops(Transit& tile, const tileTransitInfo& tile_info) {
  const auto& tile_children = tile_info.station_children;
  auto node_id = tile_info.graphid;
  feedCache feeds_cache;

  // loop through all stations inside the tile, and write PBF nodes in the order that is expected
  std::unordered_map<feedObject, GraphId> platform_node_ids;
  for (const feedObject& station : tile_info.stations) {
    const auto& feed = feeds_cache(station);
    auto station_as_stop = feed.get_stop(station.id);

    // Add the Egress
    int node_count = tile.nodes_size();
    for (const auto& child : tile_children) {
      auto child_stop = feed.get_stop(child.second);
      if (child.first.id == station.id &&
          child_stop->location_type == gtfs::StopLocationType::EntranceExit) {
        setup_stops(tile, *child_stop, node_id, platform_node_ids, station.feed,
                    NodeType::kTransitEgress, false);
      }
    }
    // We require an in/egress so if we didnt add one we need to fake one
    if (tile.nodes_size() == node_count) {
      setup_stops(tile, *station_as_stop, node_id, platform_node_ids, station.feed,
                  NodeType::kTransitEgress, true);
    }

    // Add the Station
    GraphId prev_id(tile.nodes(node_count).graphid());
    if (station_as_stop->location_type == gtfs::StopLocationType::Station) {
      // TODO(nils): what happens to the station if it was actually in another tile but still recorded
      // here (see above)?!
      setup_stops(tile, *station_as_stop, node_id, platform_node_ids, station.feed,
                  NodeType::kTransitStation, false, prev_id);
    } else {
      // if there was a platform/egress with no parent station, we add one
      setup_stops(tile, *station_as_stop, node_id, platform_node_ids, station.feed,
                  NodeType::kTransitStation, true, prev_id);
    }

    // Add the Platform
    node_count = tile.nodes_size();
    prev_id = GraphId(tile.nodes().rbegin()->graphid());
    for (const auto& child : tile_children) {
      auto child_stop = feed.get_stop(child.second);

      if (child.first.id == station.id &&
          child_stop->location_type == gtfs::StopLocationType::StopOrPlatform) {
        setup_stops(tile, *child_stop, node_id, platform_node_ids, station.feed,
                    NodeType::kMultiUseTransitPlatform, false, prev_id);
      }
    }
    // We require platforms as well so if we didnt add at least 1 we need to fake one now
    if (tile.nodes_size() == node_count) {
      setup_stops(tile, *station_as_stop, node_id, platform_node_ids, station.feed,
                  NodeType::kMultiUseTransitPlatform, true, prev_id);
    }
  }
  return platform_node_ids;
}

// read feed data per stop, given shape
float add_stop_pair_shapes(const gtfs::Stop& stop_connect,
                           const gtfs::Shape& trip_shape,
                           const gtfs::StopTime& pointStopTime) {
  // check which segment would belong to which tile
  if (pointStopTime.shape_dist_traveled > 0) {
    return pointStopTime.shape_dist_traveled;
  }
  float dist_traveled = 0;
  float min_sq_distance = INFINITY;
  PointLL stopPoint = PointLL(stop_connect.stop_lon, stop_connect.stop_lat);
  projector_t project(stopPoint);
  for (int segment = 0; segment < static_cast<int>(trip_shape.size()) - 1; segment++) {
    auto currOrigin = trip_shape[segment];
    auto currDest = trip_shape[segment + 1];
    PointLL originPoint = PointLL(currOrigin.shape_pt_lon, currOrigin.shape_pt_lat);
    PointLL destPoint = PointLL(currDest.shape_pt_lon, currDest.shape_pt_lat);

    PointLL minPoint = project(originPoint, destPoint);
    float sq_distance = project.approx.DistanceSquared(minPoint);
    if (sq_distance < min_sq_distance) {
      min_sq_distance = dist_traveled + originPoint.Distance(minPoint);
    }
    dist_traveled += originPoint.Distance(destPoint);
  }
  return dist_traveled;
}

// return dangling stop_pairs, write stop data from feed
bool write_stop_pairs(Transit& tile,
                      const tileTransitInfo& tile_info,
                      std::unordered_map<feedObject, GraphId> platform_node_ids,
                      unique_transit_t& uniques) {

  const auto& tile_tripIds = tile_info.trips;
  feedCache tripFeeds;
  bool dangles = false;

  // converts service start/end dates of the form (yyyymmdd) into epoch seconds
  auto to_local_epoch_sec = [](const std::string& dt) -> uint32_t {
    date::local_seconds tp;
    std::istringstream in{dt};
    in >> date::parse("%Y%m%d", tp);
    return static_cast<uint32_t>(tp.time_since_epoch().count());
  };

  for (const feedObject& feed_trip : tile_tripIds) {
    const auto& tile_tripId = feed_trip.id;
    const std::string currFeedPath = feed_trip.feed;
    const auto& feed = tripFeeds(feed_trip);

    auto currTrip = feed.get_trip(tile_tripId);

    // already sorted by stop_sequence
    const auto tile_stopTimes = feed.get_stop_times_for_trip(tile_tripId);

    for (size_t stop_sequence = 0; stop_sequence < tile_stopTimes.size() - 1; stop_sequence++) {
      gtfs::StopTime origin_stopTime = tile_stopTimes[stop_sequence];
      gtfs::Id origin_stopId = origin_stopTime.stop_id;
      auto origin_stop = feed.get_stop(origin_stopId);
      assert(origin_stop);
      gtfs::StopTime dest_stopTime = tile_stopTimes[stop_sequence + 1];
      gtfs::Id dest_stopId = dest_stopTime.stop_id;
      auto dest_stop = feed.get_stop(dest_stopId);
      assert(dest_stop);
      auto origin_graphid_it = platform_node_ids.find({origin_stopId, currFeedPath});
      auto dest_graphid_it = platform_node_ids.find({dest_stopId, currFeedPath});
      const bool origin_is_in_tile = origin_graphid_it != platform_node_ids.end();
      const bool dest_is_in_tile = dest_graphid_it != platform_node_ids.end();

      // check if this stop_pair (the origin of the pair) is inside the current tile
      if ((origin_is_in_tile || dest_is_in_tile) &&
          origin_stopTime.trip_id == dest_stopTime.trip_id) {

        dangles = dangles || !origin_is_in_tile || !dest_is_in_tile;

        auto* stop_pair = tile.add_stop_pairs();
        stop_pair->set_bikes_allowed(currTrip->bikes_allowed == gtfs::TripAccess::Yes);
        const gtfs::Shape& currShape = feed.get_shape(currTrip->shape_id);

        if (currTrip->block_id != "") {
          uniques.lock.lock();
          auto inserted =
              uniques.block_ids.insert({currTrip->block_id, uniques.block_ids.size() + 1});
          stop_pair->set_block_id(inserted.first->second);
          uniques.lock.unlock();
        }

        bool origin_is_generated = tile_info.station_children.find({origin_stopId, currFeedPath}) !=
                                   tile_info.station_children.end();
        auto origin_onestop_id =
            origin_is_generated ? origin_stopId
                                : origin_stopId + "_" + to_string(NodeType::kMultiUseTransitPlatform);
        stop_pair->set_origin_onestop_id(origin_onestop_id);

        bool dest_is_generated = tile_info.station_children.find({dest_stopId, currFeedPath}) !=
                                 tile_info.station_children.end();
        auto dest_onestop_id =
            dest_is_generated ? dest_stopId
                              : dest_stopId + "_" + to_string(NodeType::kMultiUseTransitPlatform);
        stop_pair->set_destination_onestop_id(dest_onestop_id);

        if (origin_is_in_tile) {
          stop_pair->set_origin_departure_time(origin_stopTime.departure_time.get_total_seconds());
          // So we looked up the node graphid by name, the name is either the actual name of the
          // platform (track 5 or something) OR its just the name of the station (in the case that the
          // platform is generated). So when its generated we will have gotten back the graphid for
          // the parent station, not for the platform, and the generated platform in that case will be
          // the next node after the station (we did this in write_stops)
          stop_pair->set_origin_graphid(origin_graphid_it->second +
                                        static_cast<uint64_t>(origin_is_generated));

          // call function to set shape
          float dist = add_stop_pair_shapes(*origin_stop, currShape, origin_stopTime);
          stop_pair->set_origin_dist_traveled(dist);
        }

        if (dest_is_in_tile) {
          stop_pair->set_destination_arrival_time(dest_stopTime.arrival_time.get_total_seconds());
          // Same as above wrt to named and unnamed (generated) platforms
          stop_pair->set_destination_graphid(dest_graphid_it->second +
                                             static_cast<uint64_t>(dest_is_generated));
          // call function to set shape
          float dist = add_stop_pair_shapes(*dest_stop, currShape, dest_stopTime);
          stop_pair->set_destination_dist_traveled(dist);
        }
        auto route_it = tile_info.routes.find({currTrip->route_id, currFeedPath});

        stop_pair->set_route_index(route_it->second);

        // add information from calendar_dates.txt
        const gtfs::CalendarDates& trip_calDates = feed.get_calendar_dates(currTrip->service_id);
        for (const auto& cal_date_item : trip_calDates) {
          auto d = to_local_epoch_sec(cal_date_item.date.get_raw_date());
          if (cal_date_item.exception_type == gtfs::CalendarDateException::Added)
            stop_pair->add_service_added_dates(d);
          else
            stop_pair->add_service_except_dates(d);
        }

        // check that a valid calendar exists for the service id
        auto trip_calendar = feed.get_calendar(currTrip->service_id);
        if (trip_calendar) {
          stop_pair->set_service_start_date(
              to_local_epoch_sec(trip_calendar->start_date.get_raw_date()));
          stop_pair->set_service_end_date(to_local_epoch_sec(trip_calendar->end_date.get_raw_date()));
        }

        // grab the headsign
        stop_pair->set_trip_headsign(currTrip->trip_headsign);

        uniques.lock.lock();
        auto inserted = uniques.trips.insert({currTrip->trip_id, uniques.trips.size()});
        stop_pair->set_trip_id(inserted.first->second);
        uniques.lock.unlock();

        stop_pair->set_wheelchair_accessible(currTrip->wheelchair_accessible ==
                                             gtfs::TripAccess::Yes);

        // get frequency info
        if (!feed.get_frequencies(currTrip->trip_id).empty()) {
          const auto& currFrequencies = feed.get_frequencies(currTrip->trip_id);
          if (currFrequencies.size() > 1) {
            LOG_WARN("More than one frequencies based schedule for " + currTrip->trip_id);
          }

          auto freq_start_time = (currFrequencies[0].start_time.get_raw_time());
          auto freq_end_time = (currFrequencies[0].end_time.get_raw_time());
          auto freq_time = freq_start_time + freq_end_time;

          // TODO: check which type of frequency it is, could be exact (meaning headway sections
          //  between trips) or schedule based (same headway all the time)

          if (currFrequencies.size() > 0) {
            stop_pair->set_frequency_end_time(DateTime::seconds_from_midnight(freq_end_time));
            stop_pair->set_frequency_headway_seconds(currFrequencies[0].headway_secs);
          }

          auto line_id = stop_pair->origin_onestop_id() < stop_pair->destination_onestop_id()
                             ? stop_pair->origin_onestop_id() + stop_pair->destination_onestop_id() +
                                   currTrip->route_id + freq_time
                             : stop_pair->destination_onestop_id() + stop_pair->origin_onestop_id() +
                                   currTrip->route_id + freq_time;
          uniques.lock.lock();
          uniques.lines.insert({line_id, uniques.lines.size()});
          uniques.lock.unlock();
        }
      }
    }
  }
  return dangles;
}

// read routes data from feed
void write_routes(Transit& tile, const tileTransitInfo& tile_info) {

  const auto& tile_routeIds = tile_info.routes;
  feedCache feedRoutes;

  // loop through all stops inside the tile

  for (const auto& feed_route : tile_routeIds) {
    const auto& tile_routeId = feed_route.first.id;
    const auto& feed = feedRoutes(feed_route.first);
    auto* route = tile.add_routes();
    auto currRoute = feed.get_route(tile_routeId);
    assert(currRoute);

    route->set_name(currRoute->route_short_name);
    route->set_onestop_id(currRoute->route_id);
    route->set_operated_by_onestop_id(currRoute->agency_id);
    auto currAgency = feed.get_agency(currRoute->agency_id);
    assert(currAgency);
    route->set_operated_by_name(currAgency->agency_name);
    route->set_operated_by_website(currAgency->agency_url);

    route->set_route_color(strtol(currRoute->route_color.c_str(), nullptr, 16));
    route->set_route_desc(currRoute->route_desc);
    route->set_route_long_name(currRoute->route_long_name);
    route->set_route_text_color(strtol(currRoute->route_text_color.c_str(), nullptr, 16));
    route->set_vehicle_type(
        (valhalla::mjolnir::Transit_VehicleType)(static_cast<int>(currRoute->route_type)));
  }
}

// grab feed data from feed
void write_shapes(Transit& tile, const tileTransitInfo& tile_info) {

  feedCache feedShapes;

  // loop through all shapes inside the tile
  for (const auto& feed_shape : tile_info.shapes) {
    const auto& tile_shape = feed_shape.first.id;
    const auto& feed = feedShapes(feed_shape.first);
    auto* shape = tile.add_shapes();
    const gtfs::Shape& currShape = feed.get_shape(tile_shape, true);
    // We use currShape[0] because 'Shape' type is a vector of ShapePoints, but they all have the same
    // shape_id
    shape->set_shape_id(feed_shape.second);
    std::vector<PointLL> trip_shape;
    for (const auto& shape_pt : currShape) {
      trip_shape.emplace_back(PointLL(shape_pt.shape_pt_lon, shape_pt.shape_pt_lat));
    }
    shape->set_encoded_shape(encode7(trip_shape));
  }
}

// pre-processes feed data and writes to the pbfs (calls the 'write' functions)
void ingest_tiles(const boost::property_tree::ptree& pt,
                  std::priority_queue<tileTransitInfo>& queue,
                  unique_transit_t& uniques,
                  std::promise<std::list<GraphId>>& promise) {

  std::list<GraphId> dangling;
  auto now = time(nullptr);
  auto* utc = gmtime(&now);
  utc->tm_year += 1900;
  ++utc->tm_mon; // TODO: use timezone code?

  auto database = pt.get_optional<std::string>("mjolnir.timezone");
  auto path = pt.get<std::string>("mjolnir.transit_feeds_dir");
  // Initialize the tz DB (if it exists)
  sqlite3* tz_db_handle = database ? GetDBHandle(*database) : nullptr;
  if (!database) {
    LOG_WARN("Time zone db not found.  Not saving time zone information from db.");
  } else if (!tz_db_handle) {
    LOG_WARN("Time zone db " + *database + " not found.  Not saving time zone information from db.");
  }
  auto tz_conn = valhalla::mjolnir::make_spatialite_cache(tz_db_handle);

  while (true) {
    tileTransitInfo current;
    uniques.lock.lock();
    if (queue.empty()) {
      uniques.lock.unlock();
      break;
    }
    current = queue.top();
    queue.pop();
    uniques.lock.unlock();

    Transit tile;
    auto file_name = GraphTile::FileSuffix(current.graphid, SUFFIX_NON_COMPRESSED);
    file_name = file_name.substr(0, file_name.size() - 3) + "pbf";
    filesystem::path transit_tile = pt.get<std::string>("mjolnir.transit_dir") +
                                    filesystem::path::preferred_separator + file_name;

    // tiles are wrote out with .pbf or .pbf.n ext
    const std::string& prefix = transit_tile.string();
    LOG_INFO("Writing " + prefix);

    std::unordered_map<feedObject, GraphId> platform_node_ids = write_stops(tile, current);
    bool dangles = write_stop_pairs(tile, current, platform_node_ids, uniques);
    write_routes(tile, current);
    write_shapes(tile, current);

    // list of dangling tiles
    if (dangles) {
      dangling.emplace_back(current.graphid);
    }

    if (tile.stop_pairs_size()) {
      write_pbf(tile, transit_tile.string());
    }
  }
  promise.set_value(dangling);
}

// connect the stop_pairs that span multiple tiles by processing dangling tiles
void stitch_tiles(const boost::property_tree::ptree& pt,
                  const std::unordered_set<GraphId>& all_tiles,
                  std::list<GraphId>& tiles,
                  std::mutex& lock) {
  auto grid = TileHierarchy::levels().back().tiles;
  auto tile_name = [&pt](const GraphId& id) {
    auto file_name = GraphTile::FileSuffix(id);
    file_name = file_name.substr(0, file_name.size() - 3) + "pbf";
    return pt.get<std::string>("mjolnir.transit_dir") + filesystem::path::preferred_separator +
           file_name;
  };

  // for each tile
  while (true) {
    GraphId current;
    lock.lock();
    if (tiles.empty()) {
      lock.unlock();
      break;
    }
    current = tiles.front();
    tiles.pop_front();
    lock.unlock();

    auto prefix = tile_name(current);
    auto file_name = prefix;
    int ext = 0;

    do {

      // open tile make a hash of missing stop to invalid graphid
      auto tile = read_pbf(file_name, lock);
      std::unordered_map<std::string, GraphId> needed;
      for (const auto& stop_pair : tile.stop_pairs()) {
        if (!stop_pair.has_origin_graphid()) {
          needed.emplace(stop_pair.origin_onestop_id(), GraphId{});
        }
        if (!stop_pair.has_destination_graphid()) {
          needed.emplace(stop_pair.destination_onestop_id(), GraphId{});
        }
      }

      // do while we have more to find and arent sick of searching
      std::set<GraphId, dist_sort_t> unchecked(all_tiles.cbegin(), all_tiles.cend(),
                                               dist_sort_t(current, grid));
      size_t found = 0;
      while (found < needed.size() && unchecked.size()) {
        // crack it open to see if it has what we want
        auto neighbor_id = *unchecked.cbegin();
        unchecked.erase(unchecked.begin());
        if (neighbor_id != current) {
          auto neighbor_file_name = tile_name(neighbor_id);
          auto neighbor = read_pbf(neighbor_file_name, lock);
          for (const auto& node : neighbor.nodes()) {
            auto platform_itr = needed.find(node.onestop_id());
            if (platform_itr != needed.cend()) {
              platform_itr->second.value = node.graphid();
              ++found;
            }
          }
        }
      }

      // get the ids fixed up and write pbf to file
      std::unordered_set<std::string> not_found;
      for (auto& stop_pair : *tile.mutable_stop_pairs()) {
        if (!stop_pair.has_origin_graphid()) {
          auto found_stop = needed.find(stop_pair.origin_onestop_id())->second;
          if (found_stop.Is_Valid()) {
            stop_pair.set_origin_graphid(found_stop);
          } else if (not_found.find(stop_pair.origin_onestop_id()) == not_found.cend()) {
            LOG_ERROR("Stop not found: " + stop_pair.origin_onestop_id());
            not_found.emplace(stop_pair.origin_onestop_id());
          }
          // else{ TODO: we could delete this stop pair }
        }
        if (!stop_pair.has_destination_graphid()) {
          auto found_stop = needed.find(stop_pair.destination_onestop_id())->second;
          if (found_stop.Is_Valid()) {
            stop_pair.set_destination_graphid(found_stop);
          } else if (not_found.find(stop_pair.destination_onestop_id()) == not_found.cend()) {
            LOG_ERROR("Stop not found: " + stop_pair.destination_onestop_id());
            not_found.emplace(stop_pair.destination_onestop_id());
          }
          // else{ TODO: we could delete this stop pair }
        }
      }
      lock.lock();
#if GOOGLE_PROTOBUF_VERSION >= 3001000
      auto size = tile.ByteSizeLong();
#else
      auto size = tile.ByteSize();
#endif
      valhalla::midgard::mem_map<char> buffer;
      // this is giving an error in the header file
      buffer.create(file_name, size);
      tile.SerializeToArray(buffer.get(), size);
      lock.unlock();
      LOG_INFO(file_name + " stitched " + std::to_string(found) + " of " +
               std::to_string(needed.size()) + " stops");

      file_name = prefix + "." + std::to_string(ext++);
    } while (filesystem::exists(file_name));
  }
}
} // namespace

namespace valhalla {
namespace mjolnir {

// thread and call ingest_tiles
std::list<GraphId> ingest_transit(const boost::property_tree::ptree& pt) {

  auto thread_count =
      pt.get<unsigned int>("mjolnir.concurrency", std::max(static_cast<unsigned int>(1),
                                                           std::thread::hardware_concurrency()));
  // go get information about what transit tiles we should be fetching
  LOG_INFO("Tiling GTFS Feeds");
  auto tiles = select_transit_tiles(pt);

  LOG_INFO("Writing " + std::to_string(tiles.size()) + " transit pbf tiles with " +
           std::to_string(thread_count) + " threads...");

  // schedule some work
  unique_transit_t uniques;
  std::vector<std::shared_ptr<std::thread>> threads(thread_count);
  std::vector<std::promise<std::list<GraphId>>> promises(threads.size());

  // ingest_tiles(pt, tiles, uniques, promises[0]);
  for (size_t i = 0; i < threads.size(); ++i) {
    threads[i].reset(new std::thread(ingest_tiles, std::cref(pt), std::ref(tiles), std::ref(uniques),
                                     std::ref(promises[i])));
  }

  // let the threads finish and get the dangling list
  for (auto& thread : threads) {
    thread->join();
  }
  std::list<GraphId> dangling;
  for (auto& promise : promises) {
    try {
      dangling.splice(dangling.end(), promise.get_future().get());
    } catch (std::exception& e) {
      // TODO: throw further up the chain?
    }
  }

  LOG_INFO("Finished");
  return dangling;
}

// thread and call stitch_tiles
void stitch_transit(const boost::property_tree::ptree& pt, std::list<GraphId>& dangling_tiles) {

  auto thread_count =
      pt.get<unsigned int>("mjolnir.concurrency", std::max(static_cast<unsigned int>(1),
                                                           std::thread::hardware_concurrency()));
  // figure out which transit tiles even exist
  filesystem::recursive_directory_iterator transit_file_itr(
      pt.get<std::string>("mjolnir.transit_dir") + filesystem::path::preferred_separator +
      std::to_string(TileHierarchy::GetTransitLevel().level));
  filesystem::recursive_directory_iterator end_file_itr;
  std::unordered_set<GraphId> all_tiles;
  for (; transit_file_itr != end_file_itr; ++transit_file_itr) {
    if (filesystem::is_regular_file(transit_file_itr->path()) &&
        transit_file_itr->path().extension() == ".pbf") {
      all_tiles.emplace(GraphTile::GetTileId(transit_file_itr->path().string()));
    }
  }

  LOG_INFO("Stitching " + std::to_string(dangling_tiles.size()) + " transit tiles with " +
           std::to_string(thread_count) + " threads...");

  // figure out where the work should go
  std::vector<std::shared_ptr<std::thread>> threads(thread_count);

  // make let them rip
  std::mutex lock;
  for (size_t i = 0; i < threads.size(); ++i) {
    threads[i].reset(new std::thread(stitch_tiles, std::cref(pt), std::cref(all_tiles),
                                     std::ref(dangling_tiles), std::ref(lock)));
  }

  // wait for them to finish
  for (auto& thread : threads) {
    thread->join();
  }

  LOG_INFO("Finished");
}

Transit read_pbf(const std::string& file_name, std::mutex& lock) {
  lock.lock();
  std::fstream file(file_name, std::ios::in | std::ios::binary);
  if (!file) {
    throw std::runtime_error("Couldn't load " + file_name);
    lock.unlock();
  }
  std::string buffer((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
  lock.unlock();
  google::protobuf::io::ArrayInputStream as(static_cast<const void*>(buffer.c_str()), buffer.size());
  google::protobuf::io::CodedInputStream cs(
      static_cast<google::protobuf::io::ZeroCopyInputStream*>(&as));
  auto limit = std::max(static_cast<size_t>(1), buffer.size() * 2);
#if GOOGLE_PROTOBUF_VERSION >= 3006000
  cs.SetTotalBytesLimit(limit);
#else
  cs.SetTotalBytesLimit(limit, limit);
#endif
  Transit transit;
  if (!transit.ParseFromCodedStream(&cs)) {
    throw std::runtime_error("Couldn't load " + file_name);
  }
  return transit;
}

Transit read_pbf(const std::string& file_name) {
  std::fstream file(file_name, std::ios::in | std::ios::binary);
  if (!file) {
    throw std::runtime_error("Couldn't load " + file_name);
  }
  std::string buffer((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
  google::protobuf::io::ArrayInputStream as(static_cast<const void*>(buffer.c_str()), buffer.size());
  google::protobuf::io::CodedInputStream cs(
      static_cast<google::protobuf::io::ZeroCopyInputStream*>(&as));
  auto limit = std::max(static_cast<size_t>(1), buffer.size() * 2);
#if GOOGLE_PROTOBUF_VERSION >= 3006000
  cs.SetTotalBytesLimit(limit);
#else
  cs.SetTotalBytesLimit(limit, limit);
#endif
  Transit transit;
  if (!transit.ParseFromCodedStream(&cs)) {
    throw std::runtime_error("Couldn't load " + file_name);
  }
  return transit;
}

// Get PBF transit data given a GraphId / tile
Transit read_pbf(const GraphId& id, const std::string& transit_dir, std::string& file_name) {
  std::string fname = GraphTile::FileSuffix(id);
  fname = fname.substr(0, fname.size() - 3) + "pbf";
  file_name = transit_dir + '/' + fname;
  Transit transit;
  transit = read_pbf(file_name);
  return transit;
}

void write_pbf(const Transit& tile, const filesystem::path& transit_tile) {
  // check for empty stop pairs and routes.
  if (tile.stop_pairs_size() == 0 && tile.routes_size() == 0 && tile.shapes_size() == 0) {
    LOG_WARN(transit_tile.string() + " had no data and will not be stored");
    return;
  }

  // write pbf to file
  if (!filesystem::exists(transit_tile.parent_path())) {
    filesystem::create_directories(transit_tile.parent_path());
  }
#if GOOGLE_PROTOBUF_VERSION >= 3001000
  auto size = tile.ByteSizeLong();
#else
  auto size = tile.ByteSize();
#endif
  valhalla::midgard::mem_map<char> buffer;
  buffer.create(transit_tile.string(), size);
  if (!tile.SerializeToArray(buffer.get(), size)) {
    LOG_ERROR("Couldn't write: " + transit_tile.string() + " it would have been " +
              std::to_string(size));
  }

  if (tile.routes_size() && tile.nodes_size() && tile.stop_pairs_size() && tile.shapes_size()) {
    LOG_INFO(transit_tile.string() + " had " + std::to_string(tile.nodes_size()) + " nodes " +
             std::to_string(tile.routes_size()) + " routes " + std::to_string(tile.shapes_size()) +
             " shapes " + std::to_string(tile.stop_pairs_size()) + " stop pairs");
  } else {
    LOG_INFO(transit_tile.string() + " had " + std::to_string(tile.stop_pairs_size()) +
             " stop pairs");
  }
}

} // namespace mjolnir
} // namespace valhalla