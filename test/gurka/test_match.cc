#include "gurka.h"
#include <gtest/gtest.h>

using namespace valhalla;

/*************************************************************/
TEST(Standalone, BasicMatch) {

  const std::string ascii_map = R"(
      1
    A---2B-3-4C
              |
              |5
              D
         )";

  const gurka::ways ways = {{"AB", {{"highway", "primary"}}},
                            {"BC", {{"highway", "primary"}}},
                            {"CD", {{"highway", "primary"}}}};

  const double gridsize = 10;
  auto map = gurka::buildtiles(ascii_map, gridsize, ways, {}, {}, "test/data/basic_match");

  auto result = gurka::match(map, {"1", "2", "3", "4", "5"}, false, "auto");

  gurka::assert::expect_route(result, {"AB", "BC", "CD"});
  gurka::assert::expect_maneuvers(result, {DirectionsLeg_Maneuver_Type_kStart,
                                           DirectionsLeg_Maneuver_Type_kContinue,
                                           DirectionsLeg_Maneuver_Type_kRight,
                                           DirectionsLeg_Maneuver_Type_kDestination});
}