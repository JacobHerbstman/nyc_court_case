# Build Council Member Roster

Builds a historical NYC Council member roster for member-deference matching.

The term table keeps all parsed source rows. The master roster uses
district-history pages where they expose term-specific member histories, because
the Legistar all-term grid can carry current district labels back across
redistricting. For districts without a parseable district-history table, the
master falls back to official Legistar office records and fills missing
1998-2001 districts from linked Legistar PersonDetail notes when available.

Rows sourced from district-history pages remain flagged for audit against Green
Book, Municipal Library, or election records before the roster is treated as
final for pre-Legistar inference.
