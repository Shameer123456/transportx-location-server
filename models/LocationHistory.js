const mongoose = require('mongoose');

/**
 * LocationHistory
 *
 * Every GPS ping from every driver is written here.
 * This is the source of truth for day trails, area coverage,
 * and future automated notifications.
 *
 * Points auto-delete after 90 days (TTL index).
 */
const locationHistorySchema = new mongoose.Schema({
  driverId:   { type: String, required: true },
  driverName: { type: String, default: '' },
  lat:        { type: Number, required: true },
  lng:        { type: Number, required: true },
  heading:    { type: Number, default: null },
  speed:      { type: Number, default: null },      // m/s from device
  timestamp:  { type: Date,   required: true },
  date:       { type: String, required: true },     // "YYYY-MM-DD" UTC — for fast day queries
});

// Primary query pattern: all points for driver X on day Y, sorted by time
locationHistorySchema.index({ driverId: 1, date: 1, timestamp: 1 });

// TTL — auto-delete after 90 days
locationHistorySchema.index({ timestamp: 1 }, { expireAfterSeconds: 90 * 24 * 60 * 60 });

module.exports = mongoose.model('LocationHistory', locationHistorySchema);
