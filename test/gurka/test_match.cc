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

  EXPECT_EQ(result.directions().routes_size(), 1);
  EXPECT_EQ(result.directions().routes(0).legs_size(), 1);

  auto& leg = result.directions().routes(0).legs(0);

  EXPECT_EQ(leg.maneuver_size(), 4);

  EXPECT_EQ(leg.maneuver(0).type(), valhalla::DirectionsLeg_Maneuver_Type_kStart);
  EXPECT_EQ(leg.maneuver(1).type(), valhalla::DirectionsLeg_Maneuver_Type_kContinue);
  EXPECT_EQ(leg.maneuver(2).type(), valhalla::DirectionsLeg_Maneuver_Type_kRight);
  EXPECT_EQ(leg.maneuver(3).type(), valhalla::DirectionsLeg_Maneuver_Type_kDestination);

  EXPECT_EQ(leg.maneuver(0).street_name(0).value(), std::string("AB"));
  EXPECT_EQ(leg.maneuver(1).street_name(0).value(), std::string("BC"));
  EXPECT_EQ(leg.maneuver(2).street_name(0).value(), std::string("CD"));
}