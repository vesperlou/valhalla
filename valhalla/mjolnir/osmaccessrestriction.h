#ifndef VALHALLA_MJOLNIR_OSMACCESSRESTRICTION_H
#define VALHALLA_MJOLNIR_OSMACCESSRESTRICTION_H

#include <cstdint>
#include <valhalla/baldr/graphconstants.h>
#include <valhalla/baldr/graphid.h>

namespace valhalla {
namespace mjolnir {

/**
 * OSM Access restriction information. Access Restrictions are stored in a
 * multimap keyed by the Id of the "from" way of the restriction.
 */
class OSMAccessRestriction {
public:
  /**
   * Constructor
   */
  OSMAccessRestriction();

  /**
   * Destructor.
   */
  ~OSMAccessRestriction();

  /**
   * Set the restriction type
   */
  void set_type(baldr::AccessType type);

  /**
   * Get the restriction type
   */
  baldr::AccessType type() const;

  /**
   * Set the value for the restriction
   */
  void set_value(uint64_t value);

  /**
   * Get the value
   */
  uint64_t value() const;

  /**
   * Get the modes for the restriction
   * @return  Returns a bit field of affected modes.
   */
  uint16_t modes() const;

  /**
   * Set the modes for the restriction
   */
  void set_modes(uint16_t modes);

  // Get the lanes mask
  uint32_t lanes() const;

  // Set the lanes mask
  void set_lanes(uint32_t lanes);

protected:
  uint64_t value_;

  uint32_t lanes_ : 20; // Mask for which lanes to apply this restriction.
  uint32_t spare_ : 12;

  struct Attributes {
    uint16_t type_ : 4;
    uint16_t modes_ : 12;
  };
  Attributes attributes_;
};

} // namespace mjolnir
} // namespace valhalla

#endif // VALHALLA_MJOLNIR_OSMACCESSRESTRICTION_H
