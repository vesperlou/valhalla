#pragma once

#include <boost/property_tree/ptree.hpp>
#include <cstdint>
#include <iostream>
#include <list>
#include <string>
#include <vector>

#include "baldr/graphconstants.h"
#include "filesystem.h"
#include "mjolnir/util.h"

#include <boost/geometry.hpp>
#include <boost/geometry/geometries/geometries.hpp>
#include <boost/geometry/geometries/register/point.hpp>
#include <boost/property_tree/ptree.hpp>

#include <geos_c.h>

namespace valhalla {
namespace mjolnir {
enum class AccessMode { ReadWriteCreate, ReadOnly, ReadWrite };

struct Landmark {
  std::string name;
  std::string type;
  double lng;
  double lat;
};

struct LandmarkDatabase {
public:
  LandmarkDatabase(const std::string& db_name, AccessMode access_mode = AccessMode::ReadOnly)
      : db(nullptr), database(db_name), access_mode_(access_mode) {
    if (!connect_database()) {
      LOG_ERROR("cannot connect to database");
    }
  }
  ~LandmarkDatabase() {
    close_database();
  }

  bool insert_landmark(const std::string& name,
                       const std::string& type,
                       const double longitude,
                       const double latitude);

  bool get_landmarks_in_bounding_box(std::vector<Landmark>* landmarks,
                                     const double minLat,
                                     const double minLong,
                                     const double maxLat,
                                     const double maxLong);

protected:
  sqlite3* db;
  sqlite3_stmt* stmt;
  uint32_t ret;
  char* err_msg = NULL;
  std::string sql;
  const std::string database;
  AccessMode access_mode_;
  int open_flags = 0;
  std::shared_ptr<void> db_conn;

  bool open_database();
  bool create_landmarks_table();
  bool create_spatial_index();
  void close_database();
  bool connect_database();
};

} // namespace mjolnir

} // namespace valhalla
