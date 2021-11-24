#include "gurka.h"
#include <gtest/gtest.h>

#if !defined(VALHALLA_SOURCE_DIR)
#define VALHALLA_SOURCE_DIR
#endif

using namespace valhalla;

std::string ascii_map = {};
gurka::ways ways = {};

class UseDirectionOnWays : public ::testing::Test {
protected:
  static gurka::map map;

  static void SetUpTestSuite() {
    ascii_map = R"(
         A--B
            |
         D--C
         |
         E
    )";

    ways = {
        {"AB", {{"highway", "motorway"}, {"ref", "US 30;US 222"}, {"direction", "East;North"}}},
        {"BC", {{"highway", "primary"}, {"int_ref", "I-95;US 40"}, {"int_direction", "North;East"}}},
        {"CD",
         {{"highway", "trunk"},
          {"ref", "US 15"},
          {"direction", "South"},
          {"int_ref", "I-80;US 15"},
          {"int_direction", "West;South"}}},
        {"DE",
         {{"highway", "secondary"},
          {"ref", "US 119"},
          {"direction", "South"},
          {"int_ref", "I-99;US XX;US 119"},
          {"int_direction", "North;;South"}}},
    };
  }
};

gurka::map UseDirectionOnWays::map = {};

/*************************************************************/

TEST_F(UseDirectionOnWays, CheckNamesAndRefsWithUseDirection) {

  const auto layout = gurka::detail::map_to_coordinates(ascii_map, 100);

  map = gurka::buildtiles(layout, ways, {}, {}, "test/data/use_direction_on_ways",
                          {{"mjolnir.data_processing.use_direction_on_ways", "true"}});

  auto result = gurka::do_action(valhalla::Options::route, map, {"A", "E"}, "auto");
  gurka::assert::osrm::expect_steps(result, {"AB", "BC", "CD", "DE"});
  gurka::assert::raw::expect_path(result,
                                  {"US 30 East/US 222 North/AB", "BC/I-95 North/US 40 East",
                                   "US 15 South/I-80 West/CD", "DE/US 119 South/I-99 North/US XX"});

  // test direction is used correctly
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(0).street_name_size(), 3);
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(0).street_name(0).value(), "US 30 East");
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(0).street_name(1).value(), "US 222 North");
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(0).street_name(2).value(), "AB");

  // int_refs are copied to refs
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(1).street_name_size(), 3);
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(1).street_name(0).value(), "BC");
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(1).street_name(1).value(), "I-95 North");
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(1).street_name(2).value(), "US 40 East");

  // ensure dups are removed
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(2).street_name_size(), 3);
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(2).street_name(0).value(), "US 15 South");
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(2).street_name(1).value(), "I-80 West");
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(2).street_name(2).value(), "CD");

  // test empty directionals
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(3).street_name_size(), 4);
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(3).street_name(0).value(), "DE");
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(3).street_name(1).value(), "US 119 South");
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(3).street_name(2).value(), "I-99 North");
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(3).street_name(3).value(), "US XX");
}

TEST_F(UseDirectionOnWays, CheckNamesAndRefsNoUseDirection) {

  const auto layout = gurka::detail::map_to_coordinates(ascii_map, 100);

  map = gurka::buildtiles(layout, ways, {}, {}, "test/data/no_use_direction_on_ways",
                          {{"mjolnir.data_processing.use_direction_on_ways", "false"}});

  auto result = gurka::do_action(valhalla::Options::route, map, {"A", "E"}, "auto");
  gurka::assert::osrm::expect_steps(result, {"AB", "BC", "CD", "DE"});
  gurka::assert::raw::expect_path(result, {"US 30/US 222/AB", "BC/I-95/US 40", "US 15/I-80/CD",
                                           "DE/US 119/I-99/US XX"});

  // test direction is used correctly
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(0).street_name_size(), 3);
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(0).street_name(0).value(), "US 30");
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(0).street_name(1).value(), "US 222");
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(0).street_name(2).value(), "AB");

  // int_refs are copied to refs
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(1).street_name_size(), 3);
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(1).street_name(0).value(), "BC");
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(1).street_name(1).value(), "I-95");
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(1).street_name(2).value(), "US 40");

  // ensure dups are removed
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(2).street_name_size(), 3);
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(2).street_name(0).value(), "US 15");
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(2).street_name(1).value(), "I-80");
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(2).street_name(2).value(), "CD");

  // test empty directionals
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(3).street_name_size(), 4);
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(3).street_name(0).value(), "DE");
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(3).street_name(1).value(), "US 119");
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(3).street_name(2).value(), "I-99");
  EXPECT_EQ(result.directions().routes(0).legs(0).maneuver(3).street_name(3).value(), "US XX");
}
