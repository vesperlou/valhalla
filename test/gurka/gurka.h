#ifndef VALHALLA_TEST_GURKA_H
#define VALHALLA_TEST_GURKA_H
/******************************************************************************
 * End-to-end tests
 *
 * These include:
 *   1. Parsing an OSM map
 *   2. Generating tiles
 *   3. Calculating routes on tiles
 *   4. Verify the expected route
 ******************************************************************************/
#include "loki/worker.h"
#include "mjolnir/util.h"
#include "odin/worker.h"
#include "thor/worker.h"
#include <boost/filesystem.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>

#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"

#include "midgard/logging.h"
#include "midgard/pointll.h"

#include <osmium/builder/attr.hpp>
#include <osmium/builder/osm_object_builder.hpp>
#include <osmium/io/pbf_output.hpp>

#include <regex>
#include <string>

#include <gtest/gtest.h>

namespace valhalla {
namespace gurka {

struct map {
  boost::property_tree::ptree config;
  std::unordered_map<std::string, midgard::PointLL> nodes;
};

using ways = std::unordered_map<std::string, std::unordered_map<std::string, std::string>>;
using nodes = std::unordered_map<std::string, std::unordered_map<std::string, std::string>>;

enum relation_member_type { node_member, way_member };
struct relation_member {
  relation_member_type type;
  std::string ref;
  std::string role;
};
struct relation {
  std::vector<relation_member> members;
  std::unordered_map<std::string, std::string> tags;
};

using relations = std::vector<relation>;

using nodelayout = std::unordered_map<std::string, midgard::PointLL>;

namespace detail {

boost::property_tree::ptree build_config(const std::string& tiledir) {

  const std::string default_config = R"(
    {"mjolnir":{"tile_dir":"", "concurrency": 1},
     "thor":{
       "logging" : {"long_request" : 100}
     },
     "meili":{
       "logging" : {"long_request" : 100},
       "grid" : {"cache_size" : 100, "size": 100 },
       "default": {
         "beta": 3,
         "breakage_distance": 2000,
         "geometry": false,
         "gps_accuracy": 5.0,
         "interpolation_distance": 10,
         "max_route_distance_factor": 5,
         "max_route_time_factor": 5,
         "max_search_radius": 100,
         "route": true,
         "search_radius": 50,
         "sigma_z": 4.07,
         "turn_penalty_factor": 0
       },
       "customizable": [
         "mode",
         "search_radius",
         "turn_penalty_factor",
         "gps_accuracy",
         "interpolation_distance",
         "sigma_z",
         "beta",
         "max_route_distance_factor",
         "max_route_time_factor"
       ]
     },
     "loki":{
       "actions": [
         "locate",
         "route",
         "height",
         "sources_to_targets",
         "optimized_route",
         "isochrone",
         "trace_route",
         "trace_attributes",
         "transit_available"
       ],
       "logging" : {"long_request" : 100}, 
       "service_defaults" : {
         "minimum_reachability" : 50,
         "radius" : 0,
         "search_cutoff" : 35000,
         "node_snap_tolerance" : 5,
         "street_side_tolerance" : 5,
         "heading_tolerance" : 60
        }
     },
     "service_limits": {
      "auto": {"max_distance": 5000000.0, "max_locations": 20,"max_matrix_distance": 400000.0,"max_matrix_locations": 50},
      "auto_shorter": {"max_distance": 5000000.0,"max_locations": 20,"max_matrix_distance": 400000.0,"max_matrix_locations": 50},
      "bicycle": {"max_distance": 500000.0,"max_locations": 50,"max_matrix_distance": 200000.0,"max_matrix_locations": 50},
      "bus": {"max_distance": 5000000.0,"max_locations": 50,"max_matrix_distance": 400000.0,"max_matrix_locations": 50},
      "hov": {"max_distance": 5000000.0,"max_locations": 20,"max_matrix_distance": 400000.0,"max_matrix_locations": 50},
      "taxi": {"max_distance": 5000000.0,"max_locations": 20,"max_matrix_distance": 400000.0,"max_matrix_locations": 50},
      "isochrone": {"max_contours": 4,"max_distance": 25000.0,"max_locations": 1,"max_time": 120},
      "max_avoid_locations": 50,"max_radius": 200,"max_reachability": 100,"max_alternates":2,
      "multimodal": {"max_distance": 500000.0,"max_locations": 50,"max_matrix_distance": 0.0,"max_matrix_locations": 0},
      "pedestrian": {"max_distance": 250000.0,"max_locations": 50,"max_matrix_distance": 200000.0,"max_matrix_locations": 50,"max_transit_walking_distance": 10000,"min_transit_walking_distance": 1},
      "skadi": {"max_shape": 750000,"min_resample": 10.0},
      "trace": {"max_distance": 200000.0,"max_gps_accuracy": 100.0,"max_search_radius": 100,"max_shape": 16000,"max_best_paths":4,"max_best_paths_shape":100},
      "transit": {"max_distance": 500000.0,"max_locations": 50,"max_matrix_distance": 200000.0,"max_matrix_locations": 50},
      "truck": {"max_distance": 5000000.0,"max_locations": 20,"max_matrix_distance": 400000.0,"max_matrix_locations": 50}
    }
  })";

  std::stringstream stream(default_config);
  boost::property_tree::ptree ptree;
  boost::property_tree::json_parser::read_json(stream, ptree);
  ptree.put("mjolnir.tile_dir", tiledir);
  return ptree;
}

std::string build_valhalla_route_request(const map& map,
                                         const std::vector<std::string>& waypoints,
                                         const std::string& costing = "auto") {

  rapidjson::StringBuffer s;
  rapidjson::Writer<rapidjson::StringBuffer> writer(s);

  writer.StartObject();
  writer.Key("locations");
  writer.StartArray();
  for (const auto& waypoint : waypoints) {
    writer.StartObject();
    writer.Key("lat");
    writer.Double(map.nodes.at(waypoint).lat());
    writer.Key("lon");
    writer.Double(map.nodes.at(waypoint).lng());
    writer.EndObject();
  }
  writer.EndArray();
  writer.Key("costing");
  writer.String(costing);
  writer.EndObject();

  return s.GetString();
}

std::string build_valhalla_match_request(const map& map,
                                         const std::vector<std::string>& waypoints,
                                         const bool break_at_points = false,
                                         const std::string& costing = "auto") {

  rapidjson::StringBuffer s;
  rapidjson::Writer<rapidjson::StringBuffer> writer(s);

  writer.StartObject();
  writer.Key("shape");
  writer.StartArray();
  for (const auto& waypoint : waypoints) {
    writer.StartObject();
    writer.Key("lat");
    writer.Double(map.nodes.at(waypoint).lat());
    writer.Key("lon");
    writer.Double(map.nodes.at(waypoint).lng());
    if (break_at_points) {
      writer.Key("type");
      writer.String("break");
    }
    writer.EndObject();
  }
  writer.EndArray();
  writer.Key("costing");
  writer.String(costing);
  writer.Key("shape_match");
  writer.String("map_snap");
  writer.EndObject();

  return s.GetString();
}

std::vector<std::string> splitter(const std::string in_pattern, const std::string& content) {
  std::vector<std::string> split_content;
  std::regex pattern(in_pattern);
  std::copy(std::sregex_token_iterator(content.begin(), content.end(), pattern, -1),
            std::sregex_token_iterator(), std::back_inserter(split_content));
  return split_content;
}

void ltrim(std::string& s) {
  s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](int ch) { return !std::isspace(ch); }));
}
void rtrim(std::string& s) {
  s.erase(std::find_if(s.rbegin(), s.rend(), [](int ch) { return !std::isspace(ch); }).base(),
          s.end());
}

std::string trim(std::string s) {
  ltrim(s);
  rtrim(s);
  return s;
}

/**
 * Given a string that's an "ASCII map", will decide on coordinates
 * for the nodes drawn on the grid.
 *
 * @returns a dictionary of node IDs to lon/lat values
 */
nodelayout map_to_coordinates(const std::string& map,
                              const double gridsize_metres,
                              const midgard::PointLL& topleft = {0, 0}) {

  // Gridsize is in meters per character

  const double earth_mean_radius = 6371008.8;
  const double DEGREE_TO_RAD = 0.017453292519943295769236907684886;
  const double metres_to_degrees = 1 / (DEGREE_TO_RAD * earth_mean_radius);
  const double grid_to_degree = gridsize_metres * metres_to_degrees;

  // Split string into lines
  // Strip whitespace lines, if they exist
  // Strip common leading whitespace
  // Decide locations for nodes based on remaining grid

  // Split string into lines
  auto lines = splitter("\n", map);
  if (lines.empty())
    return {};

  // Remove the leading whitespace lines, if they exist
  while (trim(lines.front()).empty()) {
    lines.erase(lines.begin());
  }

  // Find out the min whitespace on each line, then remove that from each line
  long min_whitespace = std::numeric_limits<long>::max();
  for (const auto& line : lines) {
    // Skip blank lines, as they might have no space at all, but shouldn't
    // be allowed to affect positioning
    if (trim(line).empty())
      continue;
    auto pos =
        std::find_if(line.begin(), line.end(), [](const auto& ch) { return !std::isspace(ch); });
    min_whitespace = std::min(min_whitespace, static_cast<long>(std::distance(line.begin(), pos)));
    if (min_whitespace == 0) // No need to continue if something is up against the left
      break;
  }

  // In-place remove leading whitespace from each line
  for (auto& line : lines) {
    // This must be a blank line or something
    if (line.size() < min_whitespace)
      continue;
    line.erase(line.begin(), line.begin() + min_whitespace);
  }

  // TODO: the way this projects coordinates onto the sphere could do with some work.
  //       it's not always going to be sensible to lay a grid down onto a sphere
  nodelayout result;
  for (int y = 0; y < lines.size(); y++) {
    for (int x = 0; x < lines[y].size(); x++) {
      auto ch = lines[y][x];
      // Only do A-Za-z0-9 for nodes - all other things are ignored
      if (std::isalnum(ch)) {
        // Always project west, then south, for consistency
        double lon = topleft.lng() + grid_to_degree * x;
        double lat = topleft.lat() - grid_to_degree * y;
        result.insert({std::string(1, ch), {lon, lat}});
      }
    }
  }

  return result;
}

/**
 * Given a map of node locations, ways, node properties and relations, will
 * generate an OSM compatible PBF file, suitable for loading into Valhalla
 */
inline void build_pbf(const nodelayout& node_locations,
                      const ways& ways,
                      const nodes& nodes,
                      const relations& relations,
                      const std::string& filename,
                      const int initial_osm_id = 0) {

  const size_t initial_buffer_size = 10000;
  osmium::memory::Buffer buffer{initial_buffer_size, osmium::memory::Buffer::auto_grow::yes};

  std::unordered_set<std::string> used_nodes;
  for (const auto& way : ways) {
    for (const auto& ch : way.first) {
      used_nodes.insert(std::string(1, ch));
    }
  }
  for (const auto& node : nodes) {
    for (const auto& ch : node.first) {
      used_nodes.insert(std::string(1, ch));
    }
  }
  for (const auto& relation : relations) {
    for (const auto& member : relation.members) {
      if (member.type == node_member) {
        used_nodes.insert(member.ref);
      }
    }
  }

  for (auto& used_node : used_nodes) {
    if (node_locations.count(used_node) == 0) {
      throw std::runtime_error("Node " + used_node + " was referred to but was not in the ASCII map");
    }
  }

  std::unordered_map<std::string, int> node_id_map;
  std::unordered_map<std::string, int> node_osm_id_map;
  int id = 0;
  for (auto& loc : node_locations) {
    node_id_map[loc.first] = id++;
  }
  int osm_id = initial_osm_id;
  for (auto& loc : node_locations) {
    if (used_nodes.count(loc.first) > 0) {
      node_osm_id_map[loc.first] = osm_id++;

      std::vector<std::pair<std::string, std::string>> tags;

      if (nodes.count(loc.first) == 0) {
        tags.push_back({"name", loc.first});
      } else {
        auto otags = nodes.at(loc.first);
        if (otags.count("name") == 0) {
          tags.push_back({"name", loc.first});
        }
        for (const auto& keyval : otags) {
          tags.push_back({keyval.first, keyval.second});
        }
      }

      osmium::builder::add_node(buffer, osmium::builder::attr::_id(node_osm_id_map[loc.first]),
                                osmium::builder::attr::_version(1),
                                osmium::builder::attr::_timestamp(std::time(nullptr)),
                                osmium::builder::attr::_location(
                                    osmium::Location{loc.second.lng(), loc.second.lat()}),
                                osmium::builder::attr::_tags(tags));
    }
  }

  std::unordered_map<std::string, int> way_osm_id_map;
  for (const auto& way : ways) {
    way_osm_id_map[way.first] = osm_id++;
    std::vector<int> nodeids;
    for (const auto& ch : way.first) {
      nodeids.push_back(node_osm_id_map[std::string(1, ch)]);
    }
    std::vector<std::pair<std::string, std::string>> tags;
    if (way.second.count("name") == 0) {
      tags.push_back({"name", way.first});
    }
    for (const auto& keyval : way.second) {
      tags.push_back({keyval.first, keyval.second});
    }
    osmium::builder::add_way(buffer, osmium::builder::attr::_id(way_osm_id_map[way.first]),
                             osmium::builder::attr::_version(1),
                             osmium::builder::attr::_timestamp(std::time(nullptr)),
                             osmium::builder::attr::_nodes(nodeids),
                             osmium::builder::attr::_tags(tags));
  }

  for (const auto& relation : relations) {

    std::vector<osmium::builder::attr::member_type> members;

    for (const auto& member : relation.members) {
      if (member.type == node_member) {
        members.push_back(
            {osmium::item_type::node, node_osm_id_map[member.ref], member.role.c_str()});
      } else {
        if (way_osm_id_map.count(member.ref) == 0) {
          throw std::runtime_error("Relation member refers to an undefined way " + member.ref);
        }
        members.push_back({osmium::item_type::way, way_osm_id_map[member.ref], member.role.c_str()});
      }
    }

    std::vector<std::pair<std::string, std::string>> tags;
    for (const auto& tag : relation.tags) {
      tags.push_back({tag.first, tag.second});
    }

    osmium::builder::add_relation(buffer, osmium::builder::attr::_id(osm_id++),
                                  osmium::builder::attr::_version(1),
                                  osmium::builder::attr::_timestamp(std::time(nullptr)),
                                  osmium::builder::attr::_members(members),
                                  osmium::builder::attr::_tags(tags));
  }

  // Create header and set generator.
  osmium::io::Header header;
  header.set("generator", "valhalla-test-creator");

  osmium::io::File output_file{filename, "pbf"};

  // Initialize Writer using the header from above and tell it that it
  // is allowed to overwrite a possibly existing file.
  osmium::io::Writer writer{output_file, header, osmium::io::overwrite::allow};

  // Write out the contents of the output buffer.
  writer(std::move(buffer));

  // Explicitly close the writer. Will throw an exception if there is
  // a problem. If you wait for the destructor to close the writer, you
  // will not notice the problem, because destructors must not throw.
  writer.close();
}

} // namespace detail

map buildtiles(const nodelayout layout,
               const ways& ways,
               const nodes& nodes,
               const relations& relations,
               const std::string& workdir) {

  map result;
  result.config = detail::build_config(workdir);
  result.nodes = layout;

  // Sanity check so that we don't blow away / by mistake
  if (workdir == "/") {
    throw std::runtime_error("Can't use / for tests, as we need to clean it out first");
  }

  if (boost::filesystem::exists(workdir))
    boost::filesystem::remove_all(workdir);
  boost::filesystem::create_directories(workdir);

  auto pbf_filename = workdir + "/map.pbf";
  std::cerr << "[          ] generating map PBF at " << pbf_filename << std::endl;
  detail::build_pbf(result.nodes, ways, nodes, relations, pbf_filename);
  std::cerr << "[          ] building tiles in " << result.config.get<std::string>("mjolnir.tile_dir")
            << std::endl;
  midgard::logging::Configure({{"type", ""}});

  mjolnir::build_tile_set(result.config, {pbf_filename}, mjolnir::BuildStage::kInitialize,
                          mjolnir::BuildStage::kValidate, false);

  return result;
}

valhalla::Api
route(const map& map, const std::vector<std::string>& waypoints, const std::string& costing) {
  std::cerr << "[          ] Routing with mjolnir.tile_dir = "
            << map.config.get<std::string>("mjolnir.tile_dir") << " with waypoints ";
  bool first = true;
  for (const auto& waypoint : waypoints) {
    if (!first)
      std::cerr << " -> ";
    std::cerr << waypoint;
    first = false;
  };
  std::cerr << " with costing " << costing << std::endl;
  auto request_json = detail::build_valhalla_route_request(map, waypoints, costing);
  std::cerr << "[          ] Valhalla request is: " << request_json << std::endl;

  valhalla::loki::loki_worker_t loki_worker(map.config);
  valhalla::thor::thor_worker_t thor_worker(map.config);
  valhalla::odin::odin_worker_t odin_worker(map.config);
  valhalla::Api request;

  valhalla::ParseApi(request_json.c_str(), valhalla::Options::route, request);
  loki_worker.route(request);
  thor_worker.route(request);
  odin_worker.narrate(request);

  return request;
}

valhalla::Api
route(const map& map, const std::string& from, const std::string& to, const std::string& costing) {
  return route(map, {from, to}, costing);
}

valhalla::Api match(const map& map,
                    const std::vector<std::string>& waypoints,
                    const bool break_at_points,
                    const std::string& costing) {
  std::cerr << "[          ] Matching with mjolnir.tile_dir = "
            << map.config.get<std::string>("mjolnir.tile_dir") << " with waypoints ";
  bool first = true;
  for (const auto& waypoint : waypoints) {
    if (!first)
      std::cerr << " -> ";
    std::cerr << waypoint;
    first = false;
  };
  std::cerr << " with costing " << costing << std::endl;
  auto request_json = detail::build_valhalla_match_request(map, waypoints, break_at_points, costing);
  std::cerr << "[          ] Valhalla request is: " << request_json << std::endl;

  valhalla::loki::loki_worker_t loki_worker(map.config);
  valhalla::thor::thor_worker_t thor_worker(map.config);
  valhalla::odin::odin_worker_t odin_worker(map.config);
  valhalla::Api request;

  valhalla::ParseApi(request_json.c_str(), valhalla::Options::trace_route, request);
  loki_worker.trace(request);
  thor_worker.trace_route(request);
  odin_worker.narrate(request);

  return request;
}

namespace assert {

void expect_route(const valhalla::Api& result, const std::vector<std::string>& expected_names) {

  EXPECT_EQ(result.directions().routes_size(), 1);
  EXPECT_EQ(result.directions().routes(0).legs_size(), 1);

  const auto& leg = result.directions().routes(0).legs(0);

  std::vector<std::string> actual_names;
  for (int i = 0; i < leg.maneuver_size(); i++) {
    if (leg.maneuver(i).street_name_size() > 0) {
      actual_names.push_back(leg.maneuver(i).street_name(0).value());
    }
  }
  auto last = std::unique(actual_names.begin(), actual_names.end());
  actual_names.erase(last, actual_names.end());

  EXPECT_EQ(actual_names, expected_names) << "Actual path didn't match expected path";
}

void expect_maneuvers(const valhalla::Api& result,
                      const std::vector<valhalla::DirectionsLeg_Maneuver_Type>& expected_maneuvers) {

  EXPECT_EQ(result.directions().routes_size(), 1);
  EXPECT_EQ(result.directions().routes(0).legs_size(), 1);

  const auto& leg = result.directions().routes(0).legs(0);

  std::vector<valhalla::DirectionsLeg_Maneuver_Type> actual_maneuvers;
  for (int i = 0; i < leg.maneuver_size(); i++) {
    actual_maneuvers.push_back(leg.maneuver(i).type());
  }

  EXPECT_EQ(actual_maneuvers, expected_maneuvers)
      << "Actual maneuvers didn't match expected maneuvers";
}

} // namespace assert

} // namespace gurka
} // namespace valhalla
#endif // VALHALLA_TEST_GURKA_H