const mongoose = require('mongoose');

const userSchema = new mongoose.Schema({
  email:        { type: String, required: true, unique: true, lowercase: true, trim: true },
  passwordHash: { type: String, required: true },
  name:         { type: String, required: true, trim: true },
  phone:        { type: String, required: true, trim: true },
  role:         { type: String, enum: ['customer', 'driver'], required: true },
  // drivers get a stable driverId for socket events (same as _id string)
  driverId:     { type: String },
}, { timestamps: true });

// Set driverId = _id string on create for drivers
userSchema.pre('save', function (next) {
  if (this.isNew && this.role === 'driver') {
    this.driverId = this._id.toString();
  }
  next();
});

module.exports = mongoose.model('User', userSchema);
