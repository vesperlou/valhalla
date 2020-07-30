#include "baldr/double_bucket_queue.h"
#include "config.h"
#include "midgard/util.h"
#include <algorithm>
#include <cmath>
#include <cstdint>
#include <iostream>
#include <vector>

#include "test.h"
#include <rapidcheck.h>

using namespace std;
using namespace valhalla;
using namespace valhalla::baldr;

namespace {

enum class AssertType {
  Gtest,
  RapidCheck,
};

void noop(){};

void TryAddRemove(std::vector<uint32_t>& costs,
                  std::vector<uint32_t>& expectedorder,
                  AssertType assert_type = AssertType::Gtest,
                  std::function<void()> tweak_data = noop) {

  std::vector<float> edgelabels;

  const auto edgecost = [&edgelabels](const uint32_t label) {
    fprintf(stderr, "##    edgecost: %i, %f\n", label, edgelabels[label]);
    return edgelabels[label];
  };

  DoubleBucketQueue adjlist(0, 10000, 1, edgecost);
  for (auto cost : costs) {

    // fprintf(stderr, "## Emplacing %i\n", cost);
    edgelabels.emplace_back(cost);
    adjlist.add(edgelabels.size() - 1);
  }
  // if (costs.size() > 0) {
  // fprintf(stderr, "First edgecost: %f\n", edgecost(0));
  //}
  // Tweak data before evaluation (if wanted)
  tweak_data();
  for (auto expected : expectedorder) {
    expected = (uint32_t)(float)expected;
    uint32_t labelindex = adjlist.pop();

    auto edgelabel = 0;
    if (labelindex != kInvalidLabel) {
      edgelabel = edgelabels[labelindex];
    } else {
      fprintf(stderr, "#####################################  INVALID LABEL INDEX\n");
      // continue;
    }

    if (assert_type == AssertType::Gtest) {
      EXPECT_EQ(edgelabel, expected) << "TryAddRemove: expected order test failed";
    } else if (assert_type == AssertType::RapidCheck) {
      RC_ASSERT(edgelabel == expected);
    } else {
      throw std::runtime_error("Missing case");
    }
  }
}

TEST(DoubleBucketQueue, TestInvalidConstruction) {
  std::vector<float> edgelabels;
  const auto edgecost = [&edgelabels](const uint32_t label) { return edgelabels[label]; };
  EXPECT_THROW(DoubleBucketQueue adjlist(0, 10000, 0, edgecost), runtime_error)
      << "Invalid bucket size not caught";
  EXPECT_THROW(DoubleBucketQueue adjlist(0, 0.0f, 1, edgecost), runtime_error)
      << "Invalid cost range not caught";
}

TEST(DoubleBucketQueue, TestAddRemove) {
  std::vector<uint32_t> costs = {67,  325, 25,  466,   1000, 100005,
                                 758, 167, 258, 16442, 278,  111111000};
  std::vector<uint32_t> expectedorder = costs;
  std::sort(expectedorder.begin(), expectedorder.end());
  TryAddRemove(costs, expectedorder);
}

TEST(DoubleBucketQueue, TestGenerated) {
  rc::check("Double bucket sorts correctly", [](const std::vector<uint32_t>& costs) {
    //RC_PRE(std::find_if(costs.begin(), costs.end(), [](uint32_t cost) -> bool {
    //         // Ignore test-data with too large values
    //         // Currently unsure if this is bug in test or actual logic
    //         return cost > 16777217;
    //       }) == costs.end());
    std::vector<uint32_t> expectedorder = costs;
    std::sort(expectedorder.begin(), expectedorder.end());
    std::vector<uint32_t> new_costs(costs);
    TryAddRemove(new_costs, expectedorder, AssertType::RapidCheck);
  });
}

//TEST(DoubleBucketQueue, TestGeneratedCostsChanging) {
//  rc::check("Double bucket sorting where costs change (like live traffic)",
//            [](const std::vector<uint32_t>& costs, const std::vector<uint32_t>& changed_costs) {
//              RC_PRE(std::find_if(costs.begin(), costs.end(), [](uint32_t cost) -> bool {
//                       // Ignore test-data with too large values
//                       // Currently unsure if this is bug in test or actual logic
//                       return cost > 16777217;
//                     }) == costs.end());
//              std::vector<uint32_t> expectedorder = costs;
//              std::sort(expectedorder.begin(), expectedorder.end());
//              std::vector<uint32_t> new_costs(costs);
//              std::vector<uint32_t> new_changed_costs(changed_costs);
//              TryAddRemove(new_costs, expectedorder, AssertType::RapidCheck,
//                           [new_costs, new_changed_costs]() { new_costs.swap(new_changed_costs); });
//            });
//}

TEST(DoubleBucketQueue, RC2Segfault) {
  std::vector<uint32_t> costs = {722947945,  1067508659, 323447915, 418158065, 741700647,  248690299,
                                 235418188,  734712702,  118457825, 265225590, 660095895,  910075485,
                                 49566967,   971490874,  940698668, 411001909, 1050928429, 58505645,
                                 592287771,  779917125,  712999537, 27030318,  589880102,  787250706,
                                 122252650,  1035447047, 595689439, 783370827, 229522080,  844634378,
                                 992574103,  782118036,  365692262, 962364499, 369724983,  662324654,
                                 574569387,  196720470,  901243917, 107854647, 95179967,   891025590,
                                 1049087461, 597334770,  542088166, 903930982, 562611418,  941419309,
                                 376685626,  743028769,  943691612, 164403943, 710579526,  369911590,
                                 18265747,   692026661,  287799556, 499679985, 502820409,  340786909,
                                 123227558,  419911472,  230737115, 532550272, 176228041,  922960173,
                                 955974888,  232644948,  690643080, 868708288, 1019099947, 829548956,
                                 602181493,  498194814,  953953989, 39490175,  117091002,  127578152,
                                 787167807,  902193305,  439270068};
  std::vector<uint32_t> expectedorder = costs;
  std::sort(expectedorder.begin(), expectedorder.end());
  TryAddRemove(costs, expectedorder);
}

TEST(DoubleBucketQueue, RC3IntegerToFloatLossyConversion) {
  // Tests what happens when the float cost can no longer represent the uint32-cost
  std::vector<uint32_t> costs = {16777217};
  std::vector<uint32_t> expectedorder = costs;
  std::sort(expectedorder.begin(), expectedorder.end());
  TryAddRemove(costs, expectedorder);
}

TEST(DoubleBucketQueue, RC4SingleBadValue) {
  // Tests what happens when the float cost can no longer represent the uint32-cost
  std::vector<uint32_t> costs = {1320209856};
  std::vector<uint32_t> expectedorder = costs;
  std::sort(expectedorder.begin(), expectedorder.end());
  TryAddRemove(costs, expectedorder);
}

void TryClear(const std::vector<uint32_t>& costs) {
  uint32_t i = 0;
  std::vector<float> edgelabels;

  const auto edgecost = [&edgelabels](const uint32_t label) { return edgelabels[label]; };
  DoubleBucketQueue adjlist(0, 10000, 50, edgecost);
  for (auto cost : costs) {
    edgelabels.emplace_back(cost);
    adjlist.add(i);
    i++;
  }
  adjlist.clear();
  uint32_t idx = adjlist.pop();
  EXPECT_EQ(idx, kInvalidLabel) << "TryClear: failed to return invalid edge index after Clear";
}

TEST(DoubleBucketQueue, TestClear) {
  std::vector<uint32_t> costs = {67,  325, 25,  466,   1000, 100005,
                                 758, 167, 258, 16442, 278,  111111000};
  TryClear(costs);
}

TEST(DoubleBucketQueue, TestCostChanging) {
  std::vector<uint32_t> costs = {67,  325, 25,  466,   1000, 100005,
                                 758, 167, 258, 16442, 278,  111111000};
  std::vector<uint32_t> expectedorder = costs;
  std::sort(expectedorder.begin(), expectedorder.end());

  {
    std::vector<float> edgelabels;

    const auto edgecost = [&edgelabels](const uint32_t label) {
      // fprintf(stderr, "##    edgecost: %i, %f\n", label, edgelabels[label]);
      return edgelabels[label];
    };

    DoubleBucketQueue adjlist(0, 10000, 1, edgecost);
    for (auto cost : costs) {

      edgelabels.emplace_back(cost);
      adjlist.add(edgelabels.size() - 1);
    }

    // Go ahead and change costs
    for (auto& cost : costs) {
      cost -= 1000;
    }

    for (auto expected : expectedorder) {
      expected = (uint32_t)(float)expected;
      uint32_t labelindex = adjlist.pop();

      auto edgelabel = 0;
      if (labelindex != kInvalidLabel) {
        edgelabel = edgelabels[labelindex];
      } else {
        fprintf(stderr, "#####################################  INVALID LABEL INDEX\n");
        // continue;
      }

      EXPECT_EQ(edgelabel, expected) << "TryAddRemove: expected order test failed";
    }
  }
}
/**
   void TestDecreseCost() {
   std::vector<uint32_t> costs = { 67, 325, 25, 466, 1000, 100005, 758, 167,
   258, 16442, 278, 111111000 };
   std::vector<uint32_t> expectedorder = costs;
   std::sort(costs);
   TryAddRemove(costs, expectedorder);

   }
*/

void TryRemove(DoubleBucketQueue& dbqueue, size_t num_to_remove, const std::vector<float>& costs) {
  auto previous_cost = -std::numeric_limits<float>::infinity();
  for (size_t i = 0; i < num_to_remove; ++i) {
    const auto top = dbqueue.pop();
    EXPECT_NE(top, kInvalidLabel) << "TryAddRemove: expected " + std::to_string(num_to_remove) +
                                         " labels to remove";
    const auto cost = costs[top];
    EXPECT_LE(previous_cost, cost) << "TryAddRemove: expected order test failed";
    previous_cost = cost;
  }

  {
    const auto top = dbqueue.pop();
    EXPECT_EQ(top, kInvalidLabel) << "Simulation: expect list to be empty";
  }
}

void TrySimulation(DoubleBucketQueue& dbqueue,
                   std::vector<float>& costs,
                   size_t loop_count,
                   size_t expansion_size,
                   size_t max_increment_cost) {
  // Track all label indexes in the dbqueue
  std::unordered_set<uint32_t> addedLabels;

  const uint32_t idx = costs.size();
  costs.push_back(10.f);
  dbqueue.add(idx);
  std::random_device rd;
  std::mt19937 gen(rd());
  for (size_t i = 0; i < loop_count; i++) {
    const auto key = dbqueue.pop();
    if (key == kInvalidLabel) {
      break;
    }

    const auto min_cost = costs[key];
    // Must be the minimal one among the tracked labels
    for (auto k : addedLabels) {
      EXPECT_LE(min_cost, costs[k]) << "Simulation: minimal cost expected";
    }
    addedLabels.erase(key);

    for (size_t i = 0; i < expansion_size; i++) {
      const auto newcost = std::floor(min_cost + 1 + test::rand01(gen) * max_increment_cost);
      if (i % 2 == 0 && !addedLabels.empty()) {
        // Decrease cost
        const auto idx =
            *std::next(addedLabels.begin(), test::rand01(gen) * (addedLabels.size() - 1));
        if (newcost < costs[idx]) {
          dbqueue.decrease(idx, newcost);
          costs[idx] = newcost;
          // todo: why commented??
          // EXPECT_EQ(dbqueue.cost(idx), newcost) << "failed to decrease cost";
        } else {
          // Assert that it must fail to decrease since costs[idx] <= newcost
          // todo: why commented??
          // EXPECT_THROW(dbqueue.decrease(idx, newcost), std::runtime_error);
          // test::assert_throw<std::runtime_error>([&dbqueue, idx, newcost](){
          //     dbqueue.decrease(idx, newcost);
          //   }, "decreasing a non-less cost must fail");
        }
      } else {
        // Add new label
        const uint32_t idx = costs.size();
        costs.push_back(newcost);
        dbqueue.add(idx);
        addedLabels.insert(idx);
      }
    }
  }

  TryRemove(dbqueue, addedLabels.size(), costs);
}

TEST(DoubleBucketQueue, TestSimulation) {
  {
    std::vector<float> costs;
    DoubleBucketQueue dbqueue1(0, 1, 100000, [&costs](const uint32_t label) { return costs[label]; });
    TrySimulation(dbqueue1, costs, 1000, 10, 1000);
  }

  {
    std::vector<float> costs;
    DoubleBucketQueue dbqueue2(0, 1, 100000, [&costs](const uint32_t label) { return costs[label]; });
    TrySimulation(dbqueue2, costs, 222, 40, 100);
  }

  {
    std::vector<float> costs;
    DoubleBucketQueue dbqueue3(0, 1, 100000, [&costs](const uint32_t label) { return costs[label]; });
    TrySimulation(dbqueue3, costs, 333, 60, 100);
  }

  {
    std::vector<float> costs;
    DoubleBucketQueue dbqueue4(0, 1, 1000, [&costs](const uint32_t label) { return costs[label]; });
    TrySimulation(dbqueue4, costs, 333, 60, 100);
  }
}

} // namespace

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
