NOTE:

"Agnostic" classes below define output that treats all vehicle types
as being the same and all zone types likewise.

In regular output, in contrast:
(1) zones are split into tram zones (and there further into tracks and platforms)
    and car zones (intrusion zones), with platforms being the only one containing
    the zone ID and numerical ID of the track to which they belong;
(2) vehicles are split into trams and cars (with in-zone information for trams
    being split into tracks and platforms, and that for cars, wrapped into an intrusion zone container).

This does add conversion overhead between the two types, but is useful for future extension of this schema
with new zone types, allowing the vehicle/zone sub-processors to stay agnostic of the vehicle's/zone's actual type,
the relevant information only being added in the main processor which stores the corresponding mappings.

TODO: move zone metadata to a top-level field
Currently, they are stored inside each zone info container in vehicle containers and in zone containers,
which leads to a lot of duplication. It is certainly best to move it to a top-level container inside the exported object.