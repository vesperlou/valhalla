#include "gurka.h"
#include <gtest/gtest.h>

using namespace valhalla;

class Reachability : public ::testing::Test {
protected:
  static gurka::map map;

  static void SetUpTestSuite() {
    constexpr double gridsize = 100;

    const std::string ascii_map = R"(
    A B    C
    | |   /
    D E1-F
    | 2 /   
    G-H
    )";

    // BE and EH are highway=path, so no cars
    // EI is a shortcut that's not accessible to bikes
    const gurka::ways ways = {{"ADG", {{"highway", "residential"}, {"service", "alley"}}},
                              {"BE", {{"highway", "residential"}, {"service", "alley"}}},
                              {"EH", {{"highway", "residential"}, {"service", "alley"}}},
                              {"GH", {{"highway", "residential"}, {"service", "alley"}}},
                              {"EF", {{"highway", "residential"}, {"service", "alley"}}},
                              {"CF", {{"highway", "residential"}, {"service", "alley"}}},
                              {"FH", {{"highway", "residential"}, {"service", "alley"}}}};
    const auto layout = gurka::detail::map_to_coordinates(ascii_map, gridsize);

    // No turns into our out of EF
    const gurka::relations relations = {{{{gurka::way_member, "BE", "from"},
                                          {gurka::way_member, "EF", "to"},
                                          {gurka::node_member, "E", "via"}},
                                         {{"type", "restriction"}, {"restriction", "no_entry"}}},
                                        {{{gurka::way_member, "EF", "from"},
                                          {gurka::way_member, "BE", "to"},
                                          {gurka::node_member, "E", "via"}},
                                         {{"type", "restriction"}, {"restriction", "no_entry"}}},
                                        {{{gurka::way_member, "EF", "from"},
                                          {gurka::way_member, "CF", "to"},
                                          {gurka::node_member, "F", "via"}},
                                         {{"type", "restriction"}, {"restriction", "no_entry"}}},
                                        {{{gurka::way_member, "CF", "from"},
                                          {gurka::way_member, "EF", "to"},
                                          {gurka::node_member, "F", "via"}},
                                         {{"type", "restriction"}, {"restriction", "no_entry"}}}};

    map = gurka::buildtiles(layout, ways, {}, relations, "test/data/reachability");
  }
};

gurka::map Reachability::map = {};

/*************************************************************/
TEST_F(Reachability, UnreachableNearest) {
  auto result = gurka::route(map, "B", "1", "auto");
  gurka::assert::expect_route(result, {"BE", "EH"});
}
TEST_F(Reachability, ReachableNearest) {
  auto result = gurka::route(map, "B", "2", "auto");
  gurka::assert::expect_route(result, {"BE", "EH"});
}