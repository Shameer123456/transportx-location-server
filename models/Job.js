const mongoose = require('mongoose');

const locationSchema = new mongoose.Schema({
  name: { type: String, default: '' },
  lat:  { type: Number, required: true },
  lng:  { type: Number, required: true },
}, { _id: false });

const jobSchema = new mongoose.Schema({
  jobId:       { type: String, required: true, unique: true },
  postedBy:    { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
  // Denormalised for fast socket reads — no populate needed
  customerName:  { type: String, required: true },
  customerPhone: { type: String, required: true },
  pickup:        { type: locationSchema, required: true },
  dropoff:       { type: locationSchema, required: true },
  description:   { type: String, default: '' },
  status:        { type: String, enum: ['open', 'closed'], default: 'open' },
  postedAt:      { type: Date, default: Date.now },
  expiresAt:     { type: Date },
}, { timestamps: true });

// Auto-expire 7 days after posting
jobSchema.pre('save', function (next) {
  if (this.isNew && !this.expiresAt) {
    this.expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000);
  }
  next();
});

// TTL index — MongoDB automatically removes expired docs
jobSchema.index({ expiresAt: 1 }, { expireAfterSeconds: 0 });
// Index for fast active-job queries
jobSchema.index({ status: 1, expiresAt: 1 });

module.exports = mongoose.model('Job', jobSchema);
