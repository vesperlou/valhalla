#include <iostream>
#include <vector>

#include "thor/alternates.h"

using namespace valhalla::thor;
using namespace valhalla::baldr;
using namespace valhalla::midgard;

/*
 * Viability tests for alternate paths based on M. Kobitzsch's Alternative Route
 * Techniques (2015). Tests verify limited sharing between segments, bounded
 * stretch, and local optimiality. Any candidate path that meets all the criteria
 * may be considered a valid alternate to the shortest path.
 */
namespace {
// Defaults thresholds
float kAtMostLonger = 1.25f; // stretch threshold
float kAtMostShared = 0.75f; // sharing threshold
float kAtLeastOptimal = 0.15f; // local optimality threshold
} // namespace

namespace valhalla {
namespace thor {
float get_max_sharing(const valhalla::Location& origin, const valhalla::Location& destination) {
  PointLL from(origin.path_edges(0).ll().lng(), origin.path_edges(0).ll().lat());
  PointLL to(destination.path_edges(0).ll().lng(), destination.path_edges(0).ll().lat());
  auto distance = from.Distance(to);

  // 10km
  if (distance < 10000.) {
    return 0.50;
  }
  // 20km
  else if (distance < 20000.) {
    return 0.60;
  }
  // 50km
  else if (distance < 50000.) {
    return 0.65;
  }
  // 100km
  else if (distance < 100000.) {
    return 0.70;
  }
  return kAtMostShared;
}

// Bounded stretch. We use cost as an approximation for stretch, to filter out
// candidate connections that are much more costly than the optimal cost. Culls
// the list of connections to only those within the stretch tolerance
void filter_alternates_by_stretch(std::vector<CandidateConnection>& connections) {
  std::sort(connections.begin(), connections.end());
  auto max_cost = connections.front().cost * kAtMostLonger;
  auto new_end = std::lower_bound(connections.begin(), connections.end(), max_cost);
  connections.erase(new_end, connections.end());
}

// Limited Sharing. Compare duration of edge segments shared between optimal path and
// candidate path. If they share more than kAtMostShared throw out this alternate.
// Note that you should recover all shortcuts before call this function.
bool validate_alternate_by_sharing(std::vector<std::unordered_set<GraphId>>& shared_edgeids,
                                   const std::vector<std::vector<PathInfo>>& paths,
                                   const std::vector<PathInfo>& candidate_path,
                                   float at_most_shared) {

  // we will calculate the overlap in edge duration between the candidate_path and paths (paths is a
  // vector of the fastest path + any alternates already chosen)
  if (paths.size() > shared_edgeids.size())
    shared_edgeids.resize(paths.size());

  // we check each accepted path against the candidate
  for (size_t i = 0; i < paths.size(); ++i) {
    // cache edge ids encountered on the current best path. Don't care about shortcuts because they
    // have already been recovered.
    auto& shared = shared_edgeids[i];
    if (shared.empty()) {
      for (const auto& pi : paths[i])
        shared.insert(pi.edgeid);
    }

    // if an edge on the candidate_path is encountered that is also on one of the existing paths,
    // we count it as a "shared" edge
    float shared_length = 0.f, total_length = 0.f;
    for (const auto& cpi : candidate_path) {
      const auto length = &cpi == &candidate_path.front()
                              ? cpi.path_distance
                              : cpi.path_distance - (&cpi - 1)->path_distance;
      total_length += length;
      if (shared.find(cpi.edgeid) != shared.end()) {
        shared_length += length;
      }
    }

    // throw this alternate away if it shares more than at_most_shared with any of the chosen paths
    assert(total_length > 0);
    if ((shared_length / total_length) > at_most_shared) {
      LOG_DEBUG("Candidate alternate rejected");
      return false;
    }
  }

  LOG_DEBUG("Candidate alternate accepted");
  // this is a viable alternate
  return true;
}

bool validate_alternate_by_local_optimality(const std::vector<PathInfo>& candidate,
                                            const GraphId& connection,
                                            const std::vector<PathInfo>& optimal_route,
                                            const valhalla::Location& origin,
                                            const valhalla::Location& destination,
                                            BidirectionalAStar& path_finding_algo,
                                            GraphReader& graphReader,
                                            valhalla::Options options,
                                            const sif::mode_costing_t& mode_costing,
                                            sif::TravelMode travel_mode,
                                            std::vector<PathInfo>& local_detour) {
  auto conn_it = std::find_if(candidate.begin(), candidate.end(), [&connection](const PathInfo& pi){
    return pi.edgeid == connection;
  });

  if (conn_it == candidate.end())
    throw std::logic_error("Didn't find connection edge in the path: path_size=" + std::to_string(candidate.size())
                           + " connedge=" + std::to_string(connection));

  uint32_t conn_idx = std::distance(candidate.begin(), conn_it);

  auto get_cost_between = [&candidate](uint32_t idx1, uint32_t idx2) -> sif::Cost {
    auto cost = candidate[idx2].elapsed_cost - candidate[idx1].transition_cost;
    if (idx1 != 0)
      cost -= candidate[idx1 - 1].elapsed_cost;
    return cost;
  };

  const float T = kAtLeastOptimal * optimal_route.back().elapsed_cost.cost;

  uint32_t left_idx = 0;
  uint32_t right_idx = conn_idx;

  while (right_idx - left_idx > 1) {
    uint32_t mid_idx = (right_idx + left_idx) / 2;
    float cost = get_cost_between(mid_idx, conn_idx).cost;

    if (cost >= T)
      left_idx = mid_idx + 1;
    else
      right_idx = mid_idx;
  }

  uint32_t first_idx = left_idx;
  if (get_cost_between(left_idx, conn_idx).cost >= T)
    first_idx = right_idx;

  // do not use first edge
  if (first_idx == 0)
    ++ first_idx;

  left_idx = conn_idx;
  right_idx = candidate.size() - 1;
  while (right_idx - left_idx > 1) {
    uint32_t mid_idx = (right_idx + left_idx) / 2;
    float cost = get_cost_between(conn_idx, mid_idx).cost;

    if (cost >= T)
      right_idx = mid_idx - 1;
    else
      left_idx = mid_idx;
  }

  uint32_t last_idx = right_idx;
  if (get_cost_between(conn_idx, right_idx).cost >= T)
    last_idx = left_idx;

  // do not use last edge
  if (last_idx + 1 == candidate.size())
    --last_idx;

  // skip local optimality check if the segment consists of one edge
  if (first_idx >= last_idx)
    return true;

  const auto alternate_detour_cost = get_cost_between(first_idx, last_idx);

  LOG_DEBUG("Local optimality check: first_idx=" + std::to_string(first_idx) + " connection=" +
           std::to_string(conn_idx) + " last_idx=" + std::to_string(last_idx) +
           " T=" + std::to_string(int(T)) + " detour_cost=" +
           std::to_string(int(alternate_detour_cost.cost)) + " detour_secs=" +
           std::to_string(int(alternate_detour_cost.secs)));

  // check if both first and last edges belong to the optimal route
  bool first_starts_at_optimal_path = std::find_if(optimal_route.begin(), optimal_route.end(),
                                       [&candidate, first_idx](const PathInfo& pi) {
                                         return pi.edgeid == candidate[first_idx].edgeid;
                                       }) != optimal_route.end();

  bool last_ends_at_optimal_path = std::find_if(optimal_route.begin(), optimal_route.end(),
                                      [&candidate, last_idx](const PathInfo& pi) {
                                        return pi.edgeid == candidate[last_idx].edgeid;
                                      }) != optimal_route.end();

  if (first_starts_at_optimal_path && last_ends_at_optimal_path) {
    LOG_WARN(
        "Candidate alternate rejected by local optimality: first and last indices lie on the optimal path");
    return false;
  }

  valhalla::Location first_location;

  if (origin.has_date_time())
    first_location.set_date_time(origin.date_time());

  {
    const auto nodeid = graphReader.GetOpposingEdge(candidate[first_idx].edgeid)->endnode();
    const auto node_ll = graphReader.GetGraphTile(nodeid)->get_node_ll(nodeid);

    first_location.mutable_ll()->set_lng(node_ll.first);
    first_location.mutable_ll()->set_lat(node_ll.second);

    auto* path_edges = first_location.mutable_path_edges();
    auto* edge = path_edges->Add();
    edge->set_graph_id(candidate[first_idx].edgeid);

    edge->set_percent_along(0.f);
    edge->set_begin_node(true);
    edge->set_end_node(false);
    edge->mutable_ll()->set_lng(node_ll.first);
    edge->mutable_ll()->set_lat(node_ll.second);
    edge->set_distance(0.f);

    for (const auto& n : graphReader.edgeinfo(candidate[first_idx].edgeid).GetNames()) {
      edge->mutable_names()->Add()->assign(n);
    }
  }

  valhalla::Location last_location;

  if (destination.has_date_time())
    last_location.set_date_time(destination.date_time());

  {
    const auto nodeid = graphReader.directededge(candidate[last_idx].edgeid)->endnode();
    const auto node_ll = graphReader.GetGraphTile(nodeid)->get_node_ll(nodeid);

    last_location.mutable_ll()->set_lng(node_ll.first);
    last_location.mutable_ll()->set_lat(node_ll.second);

    auto* path_edges = last_location.mutable_path_edges();
    auto* edge = path_edges->Add();
    edge->set_graph_id(candidate[last_idx].edgeid);

    edge->set_percent_along(1.f);
    edge->set_begin_node(false);
    edge->set_end_node(true);
    edge->mutable_ll()->set_lng(node_ll.first);
    edge->mutable_ll()->set_lat(node_ll.second);
    edge->set_distance(0.f);

    for (const auto& n : graphReader.edgeinfo(candidate[first_idx].edgeid).GetNames()) {
      edge->mutable_names()->Add()->assign(n);
    }
  }

  // we only need one shortest path
  options.clear_alternates();

  path_finding_algo.Clear();
  auto paths = path_finding_algo.GetBestPath(first_location, last_location, graphReader, mode_costing, travel_mode, options);

  if (!paths.empty()) {
    local_detour = paths.front();
    LOG_INFO("optimal detour: size=" + std::to_string(local_detour.size()) + " cost=" +
             std::to_string(int(local_detour.back().elapsed_cost.cost)) + " secs=" +
             std::to_string(int(local_detour.back().elapsed_cost.secs)));

    if (local_detour.size() != (last_idx - first_idx + 1) &&
        !std::equal(local_detour.begin(), local_detour.end(), candidate.begin() + first_idx, [](const PathInfo& a, const PathInfo& b) {
          return a.edgeid == b.edgeid;
        })) {
      const auto cost_diff = alternate_detour_cost - local_detour.back().elapsed_cost;
      LOG_WARN(
          "Candidate alternate rejected by local optimality: detour is not optimal, cost_diff=" +
          std::to_string(int(cost_diff.cost)) + " secs_diff=" + std::to_string(int(cost_diff.secs)));
      return false;
    }
  }

  return true;
}
} // namespace thor
} // namespace valhalla
