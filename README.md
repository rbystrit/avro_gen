AVRO-GEN
========

[![Build Status](https://travis-ci.org/rbystrit/avro_gen.svg?branch=master)](https://travis-ci.org/rbystrit/avro_gen)
[![codecov](https://codecov.io/gh/rbystrit/avro_gen/branch/master/graph/badge.svg)](https://codecov.io/gh/rbystrit/avro_gen)
##### Avro record class and specific record reader generator.

Current Avro implementation in Python is completely typelss and operates on dicts. 
While in many cases this is convenient and pythonic, not being able to discover the schema
by looking at the code, not enforcing schema during record constructions, and not having any 
context help from the IDE could hamper developer performance and introduce bugs. 

This project aims to rectify this situation by providing a generator for constructing concrete
record classes and constructing a reader which wraps Avro DatumReader and returns concrete classes
instead of dicts. In order not to violate Avro internals, this functionality is built strictly
on top of the DatumReader and all the specific record classes dict wrappers which define accessor
properties with proper type hints for each field in the schema. For this exact reason the 
generator does not provide an overloaded DictWriter; each specific record appears just to be a 
regular dictionary.

This is a fork of [https://github.com/rbystrit/avro_gen](https://github.com/rbystrit/avro_gen).
It adds better Python 3 support, including types, better namespace handling, support for
documentation generation, and JSON (de-)serialization.

```sh
pip install avro-gen3
```
 
##### Usage:
    schema_json = "....."
    output_directory = "....."
    from avrogen import write_schema_files
    
    write_schema_files(schema_json, output_directory)
    
The generator will create output directory if it does not exist and put generated files there. 
The generated files will be:

>  OUTPUT_DIR
>  + \_\_init\_\_.py   
>  + schema_classes.py 
>  + submodules*
 
In order to deal with Avro namespaces, since python doesn't support circular imports, the generator
 will emit all records into schema_classes.py as nested classes. The top level class there will be
 SchemaClasses, whose children will be classes representing namespaces. Each namespace class will 
 in turn contain classes for records belonging to that namespace. 
 
 Consider following schema:
 
     {"type": "record", "name": "tweet", "namespace": "com.twitter.avro", "fields": [{"name": "ID", "type": "long" }
 
 Then schema_classes.py would contain:
 
    class SchemaClasses(object):
        class com(object):
            class twitter(object):
                class acro(object):
                    class tweetClass(DictWrapper):
                        def __init__(self, inner_dict=None):
                            ....
                        @property
                        def ID(self):
                            """
                            :rtype: long
                            """
                            return self._inner_dict.get('ID', None)
                        
                        @ID.setter
                        def ID(self, value):
                            #"""
                            #:param long value:
                            #"""
                            self._inner_dict['ID'] = value                        
    
 In order to map specific record types and namespaces to modules, so that proper importing can
 be supported, there generator will create a sub-module under the output directory for each namespace
 which will export names of all types contained in that namespace. Types declared with empty 
 namespace will be exported from the root module. 
 
 So for the example above, output directory will look as follows:
 
 >  OUTPUT_DIR
 >  + \_\_init\_\_.py
 >  + schema_classes.py
 >  + com
 >   + twitter
 >     + avro
 >       + \_\_init\_\_.py  

The contents of OUTPUT_DIR/com/twitter/avro/\_\_init\_\_.py will be:
    
    from ....schema_classes import SchemaClasses
    tweet = SchemaClasses.com.twitter.avro.tweet
    
So in your code you will be able to say:
    
    from OUTPUT_DIR.com.twitter.avro import tweet
    from OUTPUT_DIR import SpecificDatumReader as TweetReader, SCHEMA as your_schema
    from avro import datafile, io
    my_tweet = tweet()
    
    my_tweet.ID = 1
    with open('somefile', 'w+b') as f:
        writer = datafile.DataFileWriter(f,io.DatumWriter(), your_schema)
        writer.append(my_tweet)
        writer.close()
    
    with open('somefile', 'rb') as f:
        reader = datafile.DataFileReader(f,TweetReader(readers_schema=your_schema))
        my_tweet1 = next(reader)
        reader.close()
        
       
### Avro protocol support

Avro protocol support is implemented the same way as schema support. To generate classes 
for a protocol:

    protocol_json = "....."
    output_directory = "....."
    from avrogen import write_protocol_files
    
    write_protocol_files(protocol_json, output_directory)
    
The structure of the generated code will be exactly same as for schema, but in addition to
regular types, *Request types will be generated in the root namespace of the protocol for each 
each message defined.

### Logical types support

Avrogen implements logical types on top of standard avro package and supports generation of 
classes thus typed. To enable logical types support, pass **use_logical_types=True** to schema 
and protocol generators. If custom logical types are implemented and such types map to types 
other than simple types or datetime.* or decimal.* then pass **custom_imports** parameter to 
generator functions so that your types are imported. Types implemented out of the box are:

- decimal (using string representation only)
- date
- time-millis
- time-micros
- timestamp-millis
- timestamp-micros

To register your custom logical type, inherit from avrogen.logical.LogicalTypeProcessor, implement
abstract methods, and add an instance to avrogen.logical.DEFAULT_LOGICAL_TYPES dictionary under the 
name of your logical type. A sample implementation looks as follows:

    class DateLogicalTypeProcessor(LogicalTypeProcessor):
        _matching_types = {'int', 'long', 'float', 'double'}
    
        def can_convert(self, writers_schema):
            return isinstance(writers_schema, schema.PrimitiveSchema) and writers_schema.type == 'int'
    
        def validate(self, expected_schema, datum):
            return isinstance(datum, datetime.date)
    
        def convert(self, writers_schema, value):
            if not isinstance(value, datetime.date):
                raise Exception("Wrong type for date conversion")
            return (value - EPOCH_DATE).total_seconds() // SECONDS_IN_DAY
    
        def convert_back(self, writers_schema, readers_schema, value):
            return EPOCH_DATE + datetime.timedelta(days=int(value))
    
        def does_match(self, writers_schema, readers_schema):
            if isinstance(writers_schema, schema.PrimitiveSchema):
                if writers_schema.type in DateLogicalTypeProcessor._matching_types:
                    return True
            return False
    
        def typename(self):
            return 'datetime.date'
    
        def initializer(self, value=None):
            return ((
                        'logical.DateLogicalTypeProcessor().convert_back(None, None, %s)' % value) if value is not None
                    else 'datetime.datetime.today().date()')


To read/write data with logical type support, use generated SpecificDatumReader 
and a LogicalDatumWriter from avro.logical.
 



    
