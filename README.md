CowSQL - a super-simple demo database
=====================================

CowSQL is a super simple id-value store that demonstrates copy-on-write, used for a live coding session at Mainstay to add copy-on-write.

It stores rows in "segments" stored in a simple TSV format, and tables are just lists of segments.

The final version (main branch) includes a "clone" operation that clones a table and will copy-on-write segments as updates are made.  This branch should be the same as devtea, the live coding branch.

See also the "pre-cow" branch for the (very similar) code before CoW was added, or "cow" for the initial feasibility check.
