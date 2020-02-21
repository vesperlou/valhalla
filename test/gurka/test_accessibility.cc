#include "gurka.h"
#include <gtest/gtest.h>

using namespace valhalla;

class SimpleRestrictions : public ::testing::Test {
protected:
  static gurka::map map;

  static void SetUpTestSuite() {
    constexpr double gridsize = 100;

    const std::string ascii_map = R"(
    A----B----C
    |    .
    D----E----F
    |    . \
    G----H----I)";

    // BE and EH are highway=path, so no cars
    const gurka::ways ways = {{"AB", {{"highway", "primary"}}},
                              {"BC", {{"highway", "primary"}}},
                              {"DEF", {{"highway", "primary"}}},
                              {"GHI", {{"highway", "primary"}}},
                              {"ADG", {{"highway", "primary"}}},
                              {"BE", {{"highway", "path"}}},
                              {"EI", {{"highway", "path"}, {"bicycle", "no"}}},
                              {"EH", {{"highway", "path"}}}};

    map = gurka::buildtiles(ascii_map, 100, ways, {}, {}, "test/data/accessibility");
  }
};

gurka::map SimpleRestrictions::map = {};

/*************************************************************/
TEST_F(SimpleRestrictions, Auto1) {
  auto result = gurka::route(map, "C", "F", "auto");
  gurka::assert::expect_route(result, {"BC", "AB", "ADG", "DEF"});
}
TEST_F(SimpleRestrictions, Auto2) {
  auto result = gurka::route(map, "C", "I", "auto");
  gurka::assert::expect_route(result, {"BC", "AB", "ADG", "GHI"});
}
TEST_F(SimpleRestrictions, WalkUsesShortcut1) {
  auto result = gurka::route(map, "C", "F", "pedestrian");
  gurka::assert::expect_route(result, {"BC", "BE", "DEF"});
}
TEST_F(SimpleRestrictions, WalkUsesBothShortcuts) {
  auto result = gurka::route(map, "C", "I", "pedestrian");
  gurka::assert::expect_route(result, {"BC", "BE", "EI"});
}
TEST_F(SimpleRestrictions, BikeUsesShortcut) {
  auto result = gurka::route(map, "C", "F", "bicycle");
  gurka::assert::expect_route(result, {"BC", "BE", "DEF"});
}
TEST_F(SimpleRestrictions, BikeAvoidsSecondShortcut) {
  auto result = gurka::route(map, "C", "I", "bicycle");
  gurka::assert::expect_route(result, {"BC", "BE", "EH", "GHI"});
}