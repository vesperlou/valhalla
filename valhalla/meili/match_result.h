// -*- mode: c++ -*-
#ifndef MMP_MATCH_RESULT_H_
#define MMP_MATCH_RESULT_H_

#include <algorithm>
#include <vector>

#include <valhalla/baldr/graphid.h>
#include <valhalla/baldr/graphreader.h>
#include <valhalla/meili/stateid.h>
#include <valhalla/midgard/pointll.h>

namespace valhalla {
namespace meili {

struct MatchResult {
  // Coordinate of the match point
  midgard::PointLL lnglat;
  // Distance from measurement to the match point
  float distance_from;
  // Which edge this match point stays
  baldr::GraphId edgeid;
  // Percentage distance along the edge
  float distance_along;
  // Optional epoch time for this match point copied from original measurement
  double epoch_time;
  // Sequential state id
  StateId stateid;

  bool HasState() const {
    return stateid.IsValid();
  }
};

struct EdgeSegment {
  EdgeSegment(baldr::GraphId the_edgeid, float the_source = 0.f, float the_target = 1.f);

  std::vector<midgard::PointLL> Shape(baldr::GraphReader& graphreader) const;

  bool Adjoined(baldr::GraphReader& graphreader, const EdgeSegment& other) const;

  // TODO make them private
  baldr::GraphId edgeid;

  float source;

  float target;
};

class NG_edge_segment {
public:
  NG_edge_segment(baldr::GraphId edgeid,
                  float source,
                  float target,
                  std::vector<MatchResult>::const_iterator first_match,
                  std::vector<MatchResult>::const_iterator last_match,
                  baldr::GraphReader& graph_reader);
  NG_edge_segment(baldr::GraphId edgeid, baldr::GraphReader& graphreader);
  NG_edge_segment(const NG_edge_segment&) = default;
  NG_edge_segment(NG_edge_segment&&) = default;
  NG_edge_segment& operator=(const NG_edge_segment&) = default;
  NG_edge_segment& operator=(NG_edge_segment&&) = default;
  ~NG_edge_segment() = default;

  std::vector<midgard::PointLL> shape() const;
  bool is_adjoined(const NG_edge_segment& other) const;
  std::pair<std::vector<MatchResult>::const_iterator, std::vector<MatchResult>::const_iterator>
  matched_results() const noexcept;
  void set_discontinuity() noexcept;
  bool has_discontinuity() const noexcept;

private:
  std::vector<MatchResult>::const_iterator first_match_;
  std::vector<MatchResult>::const_iterator last_match_;
  baldr::GraphReader* graph_reader_{nullptr};
  baldr::GraphId edgeid_{baldr::kInvalidGraphId};
  float source_{0.f};
  float target_{1.f};
  bool discontinuity_{false};
};

struct MatchResults {
  MatchResults(std::vector<MatchResult>&& results, std::vector<EdgeSegment>&& segments, float score)
      : results(results), segments(segments), score(score) {
    edges.reserve(this->segments.size());
    for (const auto& segment : this->segments)
      if (edges.empty() || edges.back() != segment.edgeid)
        edges.push_back(segment.edgeid);
    e1 = this->segments.empty() || this->segments.front().source < 1.0f ? edges.cbegin()
                                                                        : edges.cbegin() + 1;
    e2 = this->segments.empty() || this->segments.back().target > 0.0f ? edges.cend()
                                                                       : edges.cend() - 1;
  }
  bool operator==(const MatchResults& p) const {
    // find the beginning of that path in this one
    auto f = std::find(e1, e2, *p.e1);
    // maybe this path starts inside of p
    if (f == e2) {
      f = std::find(p.e1, p.e2, *e1);
      // if this path started inside of p, and whats left of p is smaller than this path, search for
      // it in this path if whats left of this path is larger than p, search for p within this
      // larger
      return f != p.e2 && p.e2 - f < e1 - e2 ? std::equal(f, p.e2, e1) : std::equal(e1, e2, f);
    }
    // p started inside of this path, if whats left of this path is smaller than p, search for it in
    // p if whats left of this path is larger than p, search for p within this larger
    return e2 - f < p.e1 - p.e2 ? std::equal(f, e2, p.e1) : std::equal(p.e1, p.e2, f);
  }

  MatchResults(const MatchResults&) = delete;
  MatchResults& operator=(const MatchResults&) = delete;
  MatchResults(MatchResults&& o) {
    results = std::move(o.results);
    segments = std::move(o.segments);
    edges = std::move(o.edges);
    score = o.score;
    e1 = segments.empty() || segments.front().source < 1.0f ? edges.cbegin() : edges.cbegin() + 1;
    e2 = segments.empty() || segments.back().target > 0.0f ? edges.cend() : edges.cend() - 1;
  }
  MatchResults& operator=(MatchResults&& o) {
    results = std::move(o.results);
    segments = std::move(o.segments);
    edges = std::move(o.edges);
    score = o.score;
    e1 = segments.empty() || segments.front().source < 1.0f ? edges.cbegin() : edges.cbegin() + 1;
    e2 = segments.empty() || segments.back().target > 0.0f ? edges.cend() : edges.cend() - 1;
    return *this;
  }

  std::vector<MatchResult> results;
  std::vector<EdgeSegment> segments;
  std::vector<uint64_t> edges;
  float score;
  std::vector<uint64_t>::const_iterator e1;
  std::vector<uint64_t>::const_iterator e2;
};

} // namespace meili
} // namespace valhalla

#endif // MMP_MATCH_RESULT_H_
