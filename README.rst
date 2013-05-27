Organising a music library can be a hassle. With the wealth of online stores all providing music tagged in various formats, it can be a nightmare to unify them all.

This is where beetFs comes in. Derived from beets, beetFs presents a FUSE filesystem that is based on your tags.

Modifying the tags within the beetFs mountpoint will not change the data on the hard disk, merely update the beet database. When an application requests a music file from within the beetFs mountpoint, beetFs provides tag information from its own database, instead of from the original file, but music data from the on-disk location.

This enables completely transparent modification of tags within an audio file with no change to the underlying on-disk data. 
