#pragma once

#include <memory>
#include <string>
#include <vector>

#include <boost/property_tree/ptree.hpp>

namespace valhalla {
namespace mjolnir {

struct Landmark {
  std::string name;
  std::string type;
  double lng;
  double lat;
};

struct LandmarkDatabase {
public:
  LandmarkDatabase(const std::string& db_name, bool read_only);
  void insert_landmark(const Landmark& landmark);
  std::vector<Landmark> get_landmarks_in_bounding_box(const double minLat,
                                                      const double minLong,
                                                      const double maxLat,
                                                      const double maxLong);

protected:
  struct db_pimpl;
  std::shared_ptr<db_pimpl> pimpl;
};

class LandmarkParser {
public:
  static std::vector<Landmark> Parse(const std::vector<std::string>& input_files);
}

} // namespace mjolnir

} // namespace valhalla
