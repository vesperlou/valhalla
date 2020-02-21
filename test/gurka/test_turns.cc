#include "gurka.h"
#include <gtest/gtest.h>

using namespace valhalla;

/*************************************************************/
TEST(Standalone, TurnStraight) {

  const std::string ascii_map = R"(
    A----B----C
         |
         D)";

  const gurka::ways ways = {{"ABC", {{"highway", "primary"}}}, {"BD", {{"highway", "primary"}}}};

  auto map = gurka::buildtiles(ascii_map, 100, ways, {}, {}, "test/data/gurka_turns_3");

  auto result = gurka::route(map, "A", "C", "auto");

  gurka::assert::expect_route(result, {"ABC"});
}
/*************************************************************/

const std::string ascii_map_1 = R"(
    A----B----C
         |
         D)";

const gurka::ways map_1_ways = {{"ABC", {{"highway", "primary"}}}, {"BD", {{"highway", "primary"}}}};

const std::string ascii_map_2 = R"(
    E----B----C
    |    |
    F----D)";

const gurka::ways map_2_ways = {{"FEBC", {{"highway", "primary"}}},
                                {"FDB", {{"highway", "primary"}}}};

class Turns : public ::testing::Test {
protected:
  static gurka::map map_1;
  static gurka::map map_2;

  static void SetUpTestSuite() {
    constexpr double gridsize = 100;

    map_1 = gurka::buildtiles(ascii_map_1, gridsize, map_1_ways, {}, {}, "test/data/gurka_turns_1");
    map_2 = gurka::buildtiles(ascii_map_2, gridsize, map_2_ways, {}, {}, "test/data/gurka_turns_2");
  }
};

gurka::map Turns::map_1 = {};
gurka::map Turns::map_2 = {};

/************************************************************************************/
TEST_F(Turns, TurnRight) {

  auto result = gurka::route(map_1, "A", "D", "auto");

  gurka::assert::expect_route(result, {"ABC", "BD"});

  gurka::assert::expect_maneuvers(result, {DirectionsLeg_Maneuver_Type_kStart,
                                           DirectionsLeg_Maneuver_Type_kRight,
                                           DirectionsLeg_Maneuver_Type_kDestination});
}
/************************************************************************************/
TEST_F(Turns, TurnLeft) {

  auto result = gurka::route(map_2, "C", "D", "auto");

  gurka::assert::expect_route(result, {"FEBC", "FDB"});

  gurka::assert::expect_maneuvers(result, {DirectionsLeg_Maneuver_Type_kStart,
                                           DirectionsLeg_Maneuver_Type_kLeft,
                                           DirectionsLeg_Maneuver_Type_kDestination});
}