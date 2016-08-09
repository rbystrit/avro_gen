AVRO-GEN
========

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
 
#####Usage:
    schema_json = "....."
    output_directory = "....."
    from avrogen import write_files
    
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
        my_tweet1 = reader.next()
        reader.close()
        
       
