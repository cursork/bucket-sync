# bucket-sync

Sync two AWS S3 buckets, even across isolated regions.

You probably don't need this unless you are operating in China and have to
sync data between an S3 bucket there and one in the rest of the world.

Note that this was designed for the requirements of a single project. No effort
has been expended in 'generalising' to cover a wider set of use-cases. It's
also unlikely that any effort will be expended to do so.

## Usage

    lein run -- someconfig.clj

Where someconfig.clj follows the pattern of config/example.clj

## Known Problems / TODO

* Uses etags to decide whether or not to sync an object. In the case of
  multipart uploads, this will be broken.

## License

MIT License. See LICENSE.

Copyright © 2015 Neil Kirsopp

