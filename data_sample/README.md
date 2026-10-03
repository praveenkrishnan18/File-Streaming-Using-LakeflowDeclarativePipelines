# Sample data

Two synthetic JSON files used to test incremental ingestion and schema evolution.

| File | Rows | Columns | Notes |
|---|---|---|---|
| `flight_bookings_day1.json` | 10 | 12 | Initial schema |
| `flight_bookings_day2.json` | 10 | 13 | Adds `LoyaltyPoints` |

Both are multiline JSON arrays, which is why the stream uses `.option("multiLine", "true")`.

**Columns (Day 1):** `BookingID`, `PassengerName`, `Email`, `Airline`, `FlightNumber`, `DepartureAirport`, `ArrivalAirport`, `BookingDate`, `DepartureDate`, `TravelClass`, `TicketPrice`, `BookingStatus`

**Note:** Day 2 contains the same 10 bookings (same `BookingID`s) as Day 1, plus the new `LoyaltyPoints` column. After both files are ingested, the Bronze table therefore holds 20 rows with each `BookingID` appearing twice. This is expected: Bronze is append-only, and de-duplication would be a Silver-layer concern.

All names and emails are generated placeholders (`user1@example.com`, etc.); no real personal data is included.
