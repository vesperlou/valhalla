#pragma once

#include <vector>

#include "thor/bidirectional_astar.h"

namespace valhalla {
namespace thor {
float get_max_sharing(const valhalla::Location& origin, const valhalla::Location& destination);

void filter_alternates_by_stretch(std::vector<CandidateConnection>& connections);

bool validate_alternate_by_sharing(std::vector<std::unordered_set<baldr::GraphId>>& shared_edgeids,
                                   const std::vector<std::vector<PathInfo>>& paths,
                                   const std::vector<PathInfo>& candidate_path,
                                   float at_most_shared);

bool validate_alternate_by_local_optimality(const std::vector<PathInfo>& candidate,
                                            const baldr::GraphId& connection,
                                            const std::vector<PathInfo>& optimal_route,
                                            const valhalla::Location& origin,
                                            const valhalla::Location& destination,
                                            BidirectionalAStar& path_finding_algo,
                                            baldr::GraphReader& graphReader,
                                            valhalla::Options options,
                                            const sif::mode_costing_t& mode_costing,
                                            sif::TravelMode travel_mode,
                                            std::vector<PathInfo>& local_detour);
} // namespace thor
} // namespace valhalla
