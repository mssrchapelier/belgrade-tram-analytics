This project uses a modified version of [SORT](https://github.com/abewley/sort) 
([Bewley et al. 2016](https://arxiv.org/abs/1602.00763)) for multi-object tracking.

The tracking algorithm itself is used without changes, but the following changes 
were made in the wrapping around it:

- Removed visualisation functions and the related imports, as well as `main()`.
- Changed the array with states and IDs returned by `Sort.update()`
  to contain the states for *all* existing trackers.
  Three sets of track IDs are returned additionally, containing the IDs
  of confirmed matched, unconfirmed matched, and unmatched tracks respectively 
  (in the original version, only confirmed tracks were returned).
  `Sort.min_hits` thus simply influences whether a track is confirmed or not.
- In `KalmanBoxTracker`, `id`s are initialised as `count + 1` instead of `count`
  and the adding of `1` to IDs is removed from `Sort.update()`.
