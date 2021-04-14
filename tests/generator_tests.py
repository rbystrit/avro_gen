import os
import unittest
import avrogen
import sys
import importlib
import shutil
from avro import io
from avro import datafile, schema
import tempfile
import avrogen.schema
import avrogen.protocol
from avrogen.dict_wrapper import DictWrapper
import logging
import sys
import datetime
import six

if not hasattr(schema, 'parse'):
    # Older versions of avro used a capital P in Parse.
    schema.parse = schema.Parse

# logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
# logging.getLogger('avrogen.schema').setLevel(logging.DEBUG)

unittest.TestLoader.sortTestMethodsUsing = None

class GeneratorTestCase(unittest.TestCase):
    TEST_NUMBER = 1

    @classmethod
    def setUpClass(cls):
        cls.SCHEMA_DIR = os.path.join(os.path.dirname(__file__), 'schemas')
        cls.TMP_DIR = os.path.join(os.path.dirname(__file__), 'avrogen_tests')
        if os.path.isdir(cls.TMP_DIR):
            shutil.rmtree(cls.TMP_DIR)
        os.mkdir(cls.TMP_DIR)
        sys.path.append(cls.TMP_DIR)

    @classmethod
    def tearDownClass(cls):
        if os.path.isdir(cls.TMP_DIR):
            shutil.rmtree(cls.TMP_DIR)

        if os.path.exists(os.path.join(os.path.dirname(__file__), 'twitter_schema')):
            shutil.rmtree(os.path.join(os.path.dirname(__file__), 'twitter_schema'))

    def setUp(self):
        tmp_dir = GeneratorTestCase.TMP_DIR
        self.test_name = 'avrogen_test' + str(GeneratorTestCase.TEST_NUMBER)
        self.output_dir = os.path.join(tmp_dir, self.test_name)
        GeneratorTestCase.TEST_NUMBER += 1

        if os.path.isdir(self.output_dir):
            shutil.rmtree(self.output_dir)
            # print __file__

    def tearDown(self):
        if os.path.isdir(self.output_dir):
            shutil.rmtree(self.output_dir)

    def read_schema(self, name):
        with open(os.path.join(GeneratorTestCase.SCHEMA_DIR, name), "r") as f:
            return f.read()
    
    def load_gen(self, test_name):
        try:
            importlib.invalidate_caches()
            root_module = importlib.import_module(test_name)
            schema_classes = importlib.import_module('.schema_classes', test_name)
            return root_module, schema_classes
        except ModuleNotFoundError as e:
            breakpoint()

    def test_dict_wrapper(self):
        a = DictWrapper.construct({'hi': 'foo'})
        b = DictWrapper.construct({'hi': 'foo'})
        c = DictWrapper.construct({'hi': 'bar'})

        self.assertEqual(a, b)
        self.assertNotEqual(a, c)

    def test_simple_record(self):
        schema_json = self.read_schema('simple_record.json')
        avrogen.schema.write_schema_files(schema_json, self.output_dir)
        root_module, schema_classes = self.load_gen(self.test_name)

        # self.assertTrue(hasattr(schema_classes, 'SchemaClasses'))
        self.assertTrue(hasattr(root_module, 'LongList'))

        long_list = root_module.LongList.construct_with_defaults()
        self.assertTrue(hasattr(long_list, 'value'))
        self.assertTrue(hasattr(long_list, 'next'))

    def test_record_with_array(self):
        schema_json = self.read_schema('record_with_array.json')
        avrogen.schema.write_schema_files(schema_json, self.output_dir)
        root_module, schema_classes = self.load_gen(self.test_name)

        # self.assertTrue(hasattr(schema_classes, 'SchemaClasses'))
        self.assertTrue(hasattr(root_module, 'LongList'))

        long_list = root_module.LongList.construct_with_defaults()
        self.assertTrue(hasattr(long_list, 'value'))
        self.assertTrue(hasattr(long_list, 'next'))
        self.assertTrue(hasattr(long_list, 'hello'))

    def test_recursive_record(self):
        schema_json = self.read_schema('recursive_record.json')
        avrogen.schema.write_schema_files(schema_json, self.output_dir)
        root_module, schema_classes = self.load_gen(self.test_name)

        # self.assertTrue(hasattr(schema_classes, 'SchemaClasses'))
        self.assertTrue(hasattr(root_module, 'LongList'))

        long_list = root_module.LongList.construct_with_defaults()
        self.assertTrue(hasattr(long_list, 'value'))
        self.assertTrue(hasattr(long_list, 'next'))

    def test_array_record(self):
        self.primitive_type_tester('array.json')

    def test_fixed(self):
        self.primitive_type_tester('fixed.json')

    def test_int(self):
        self.primitive_type_tester('fixed.json')

    def test_enum(self):
        schema_json = self.read_schema('enum.json')
        avrogen.schema.write_schema_files(schema_json, self.output_dir)
        root_module, _ = self.load_gen(self.test_name)

        self.assertTrue(hasattr(root_module, 'myenum'))

    def test_tweet(self):
        schema_json = self.read_schema('tweet.json')
        avrogen.schema.write_schema_files(schema_json, self.output_dir)
        root_module, _ = self.load_gen(self.test_name)
        twitter_ns = importlib.import_module('.com.bifflabs.grok.model.twitter.avro', self.test_name)
        common_ns = importlib.import_module('.com.bifflabs.grok.model.common.avro', self.test_name)

        self.assertTrue(hasattr(twitter_ns, 'AvroTweet'))
        self.assertTrue(hasattr(twitter_ns, 'AvroTweetMetadata'))
        self.assertTrue(hasattr(common_ns, 'AvroPoint'))
        self.assertTrue(hasattr(common_ns, 'AvroDateTime'))
        self.assertTrue(hasattr(common_ns, 'AvroKnowableOptionString'))
        self.assertTrue(hasattr(common_ns, 'AvroKnowableListString'))
        self.assertTrue(hasattr(common_ns, 'AvroKnowableBoolean'))
        self.assertTrue(hasattr(common_ns, 'AvroKnowableOptionPoint'))

        tweet = twitter_ns.AvroTweet.construct_with_defaults()
        tweet_meta = twitter_ns.AvroTweetMetadata.construct_with_defaults()
        point = common_ns.AvroPoint.construct_with_defaults()
        adt = common_ns.AvroDateTime.construct_with_defaults()
        kos = common_ns.AvroKnowableOptionString.construct_with_defaults()
        kls = common_ns.AvroKnowableListString.construct_with_defaults()
        kb = common_ns.AvroKnowableBoolean.construct_with_defaults()
        kop = common_ns.AvroKnowableOptionString.construct_with_defaults()

        self.assertIsNotNone(twitter_ns.AvroTweet.RECORD_SCHEMA)
        self.assertIsNotNone(twitter_ns.AvroTweetMetadata.RECORD_SCHEMA)
        self.assertIsNotNone(common_ns.AvroPoint.RECORD_SCHEMA)
        self.assertIsNotNone(common_ns.AvroDateTime.RECORD_SCHEMA)
        self.assertIsNotNone(common_ns.AvroKnowableOptionString.RECORD_SCHEMA)
        self.assertIsNotNone(common_ns.AvroKnowableListString.RECORD_SCHEMA)
        self.assertIsNotNone(common_ns.AvroKnowableBoolean.RECORD_SCHEMA)
        self.assertIsNotNone(common_ns.AvroKnowableOptionString.RECORD_SCHEMA)

        self.assertTrue(hasattr(tweet, 'ID'))
        self.assertTrue(hasattr(tweet, 'text'))
        self.assertTrue(hasattr(tweet, 'authorScreenName'))
        self.assertTrue(hasattr(tweet, 'authorProfileImageURL'))
        self.assertTrue(hasattr(tweet, 'authorUserID'))
        self.assertTrue(hasattr(tweet, 'location'))
        self.assertTrue(hasattr(tweet, 'placeID'))
        self.assertTrue(hasattr(tweet, 'createdAt'))
        self.assertTrue(hasattr(tweet, 'metadata'))

        self.assertTrue(hasattr(tweet_meta, 'inReplyToScreenName'))
        self.assertTrue(hasattr(tweet_meta, 'mentionedScreenNames'))
        self.assertTrue(hasattr(tweet_meta, 'links'))
        self.assertTrue(hasattr(tweet_meta, 'hashtags'))
        self.assertTrue(hasattr(tweet_meta, 'isBareCheckin'))
        self.assertTrue(hasattr(tweet_meta, 'isBareRetweet'))
        self.assertTrue(hasattr(tweet_meta, 'isRetweet'))
        self.assertTrue(hasattr(tweet_meta, 'venueID'))
        self.assertTrue(hasattr(tweet_meta, 'venuePoint'))

        self.assertTrue(hasattr(point, 'latitude'))
        self.assertTrue(hasattr(point, 'longitude'))

        self.assertTrue(hasattr(adt, 'dateTimeString'))

        self.assertTrue(hasattr(kos, 'known'))
        self.assertTrue(hasattr(kos, 'data'))

        self.assertTrue(hasattr(kls, 'known'))
        self.assertTrue(hasattr(kls, 'data'))

        self.assertTrue(hasattr(kb, 'known'))
        self.assertTrue(hasattr(kb, 'data'))

        self.assertTrue(hasattr(kop, 'known'))
        self.assertTrue(hasattr(kop, 'data'))

    @unittest.skip("don't care about logical types")
    def test_logical(self):
        schema_json = self.read_schema('logical_types.json')
        avrogen.schema.write_schema_files(schema_json, self.output_dir, use_logical_types=True)
        root_module = importlib.import_module(self.test_name)
        importlib.import_module('.schema_classes', self.test_name)

        self.assertTrue(hasattr(root_module, 'LogicalTypesTest'))
        LogicalTypesTest = root_module.LogicalTypesTest

        instance = LogicalTypesTest()

        import decimal
        import datetime
        import pytz
        import tzlocal

        self.assertIsInstance(instance.decimalField, decimal.Decimal)
        self.assertIsInstance(instance.decimalFieldWithDefault, decimal.Decimal)
        self.assertIsInstance(instance.dateField, datetime.date)
        self.assertIsInstance(instance.dateFieldWithDefault, datetime.date)
        self.assertIsInstance(instance.timeMillisField, datetime.time)
        self.assertIsInstance(instance.timeMillisFieldWithDefault, datetime.time)
        self.assertIsInstance(instance.timeMicrosField, datetime.time)
        self.assertIsInstance(instance.timeMicrosFieldWithDefault, datetime.time)
        self.assertIsInstance(instance.timestampMillisField, datetime.datetime)
        self.assertIsInstance(instance.timestampMillisFieldWithDefault, datetime.datetime)
        self.assertIsInstance(instance.timestampMicrosField, datetime.datetime)
        self.assertIsInstance(instance.timestampMicrosFieldWithDefault, datetime.datetime)

        self.assertEquals(instance.decimalFieldWithDefault, decimal.Decimal(10))
        self.assertEquals(instance.dateFieldWithDefault, datetime.date(1970, 2, 12))
        self.assertEquals(instance.timeMillisFieldWithDefault, datetime.time(second=42))
        self.assertEquals(instance.timeMicrosFieldWithDefault, datetime.time(second=42))

        self.assertEquals(
            tzlocal.get_localzone().localize(instance.timestampMicrosFieldWithDefault).astimezone(pytz.UTC),
            datetime.datetime(1970, 1, 1, 0, 0, 42, tzinfo=pytz.UTC))
        self.assertEquals(
            tzlocal.get_localzone().localize(instance.timestampMillisFieldWithDefault).astimezone(pytz.UTC),
            datetime.datetime(1970, 1, 1, 0, 0, 42, tzinfo=pytz.UTC))

    @unittest.skip("don't care about protocol tests")
    def test_simple_protocol(self):
        schema_json = self.read_schema('sample.avpr')
        avrogen.protocol.write_protocol_files(schema_json, self.output_dir)

        root_module = importlib.import_module(self.test_name)
        importlib.import_module('.schema_classes', self.test_name)
        sample_ns = importlib.import_module('.org.sample', self.test_name)
        self.assertTrue(hasattr(sample_ns, 'Account'))
        self.assertTrue(hasattr(sample_ns, 'addAccountRequest'))

        a = sample_ns.Account()
        r = sample_ns.addAccountRequest()

        self.assertTrue(hasattr(a, 'id'))
        self.assertTrue(hasattr(a, 'name'))
        self.assertTrue(hasattr(a, 'description'))

        self.assertTrue(hasattr(r, 'name'))
        self.assertTrue(hasattr(r, 'description'))

    @unittest.skip("don't care about protocol tests")
    def test_simple_protocol_inline_response(self):
        schema_json = self.read_schema('sample_inline_response_type.avpr')
        avrogen.protocol.write_protocol_files(schema_json, self.output_dir)

        root_module = importlib.import_module(self.test_name)
        importlib.import_module('.schema_classes', self.test_name)
        sample_ns = importlib.import_module('.org.sample', self.test_name)
        self.assertTrue(hasattr(sample_ns, 'Account'))
        self.assertTrue(hasattr(sample_ns, 'addAccountRequest'))

        a = sample_ns.Account()
        r = sample_ns.addAccountRequest()

        self.assertTrue(hasattr(a, 'id'))
        self.assertTrue(hasattr(a, 'name'))
        self.assertTrue(hasattr(a, 'description'))

        self.assertTrue(hasattr(r, 'name'))
        self.assertTrue(hasattr(r, 'description'))

    @unittest.skip("don't care about protocol tests")
    def test_simple_protocol_diff_ns(self):
        schema_json = self.read_schema('sample_diff_ns.avpr')
        avrogen.protocol.write_protocol_files(schema_json, self.output_dir)

        root_module = importlib.import_module(self.test_name)
        importlib.import_module('.schema_classes', self.test_name)
        sample_ns = importlib.import_module('.org.sample', self.test_name)
        com_ns = importlib.import_module('.com.sample', self.test_name)
        net_ns = importlib.import_module('.net.sample', self.test_name)
        self.assertFalse(hasattr(sample_ns, 'Account'))
        self.assertTrue(hasattr(com_ns, 'Account'))
        self.assertTrue(hasattr(net_ns, 'Account'))
        self.assertTrue(hasattr(sample_ns, 'addAccountRequest'))

        a = com_ns.Account()
        r = sample_ns.addAccountRequest()

        self.assertTrue(hasattr(a, 'id'))
        self.assertTrue(hasattr(a, 'name'))
        self.assertTrue(hasattr(a, 'description'))

        self.assertTrue(hasattr(r, 'name'))
        self.assertTrue(hasattr(r, 'description'))

    @unittest.skip("don't care about protocol tests")
    def test_simple_protocol_empty_request(self):
        schema_json = self.read_schema('sample_empty_request.avpr')
        avrogen.protocol.write_protocol_files(schema_json, self.output_dir)

        root_module = importlib.import_module(self.test_name)
        importlib.import_module('.schema_classes', self.test_name)
        sample_ns = importlib.import_module('.org.sample', self.test_name)
        self.assertTrue(hasattr(sample_ns, 'Account'))
        self.assertTrue(hasattr(sample_ns, 'addAccountRequest'))

        a = sample_ns.Account()
        r = sample_ns.addAccountRequest()

        self.assertTrue(hasattr(a, 'id'))
        self.assertTrue(hasattr(a, 'name'))
        self.assertTrue(hasattr(a, 'description'))

    @unittest.skip("don't care about protocol tests")
    def test_simple_protocol_null_response(self):
        schema_json = self.read_schema('sample_null_response.avpr')
        avrogen.protocol.write_protocol_files(schema_json, self.output_dir)

        root_module, schema_classes = self.load_gen(self.test_name)
        sample_ns = importlib.import_module('.org.sample', self.test_name)
        self.assertFalse(hasattr(sample_ns, 'Account'))
        self.assertTrue(hasattr(sample_ns, 'addAccountRequest'))

        r = sample_ns.addAccountRequest()

    def test_tweet_roundrip(self):
        schema_json = self.read_schema('tweet.json')
        output_name = os.path.join(os.path.dirname(__file__), 'twitter_schema')
        if os.path.isdir(output_name):
            shutil.rmtree(output_name)

        avrogen.schema.write_schema_files(schema_json, output_name)
        from twitter_schema.com.bifflabs.grok.model.twitter.avro import AvroTweet, AvroTweetMetadata
        from twitter_schema.com.bifflabs.grok.model.common.avro import AvroPoint, AvroDateTime, \
            AvroKnowableOptionString, AvroKnowableListString, AvroKnowableBoolean, AvroKnowableOptionPoint
        from twitter_schema import SpecificDatumReader

        tweet = AvroTweet.construct_with_defaults()
        tweet.ID = 1
        tweet.text = "AvroGenTest"
        tweet.authorScreenName = 'AvrogenTestName'
        tweet.authorProfileImageURL = 'http://'
        tweet.authorUserID = 2

        tweet.location = AvroPoint.construct_with_defaults()
        tweet.location.latitude = 1.0
        tweet.location.longitude = 2.0
        tweet.placeID = "Ether"
        tweet.createdAt = AvroDateTime.construct_with_defaults()
        tweet.createdAt.dateTimeString = "2016-10-10 10:10:10"

        tweet.metadata.inReplyToScreenName.known = False
        tweet.metadata.inReplyToScreenName.data = "What?"

        tweet.metadata.mentionedScreenNames.known = True
        tweet.metadata.mentionedScreenNames.data = ['Avro', 'Gen', 'Test']

        tweet.metadata.links.known = False
        tweet.metadata.links.data = []

        tweet.metadata.hashtags.known = False
        tweet.metadata.hashtags.data = ['###']

        tweet.metadata.isBareCheckin.known = False
        tweet.metadata.isBareCheckin.data = True

        tweet.metadata.isBareRetweet.known = True
        tweet.metadata.isBareRetweet.data = False

        tweet.metadata.isRetweet.known = True
        tweet.metadata.isRetweet.data = True

        tweet.metadata.venueID.known = False
        tweet.metadata.venueID.data = None

        tweet.metadata.venuePoint.known = False
        tweet.metadata.venuePoint.data = None

        tmp_file = tempfile.mktemp()
        with open(tmp_file, "w+b") as f:
            df = datafile.DataFileWriter(f, io.DatumWriter(), schema.parse(schema_json))
            df.append(tweet.to_avro_writable())
            df.close()

        with open(tmp_file, "rb") as f:
            df = datafile.DataFileReader(f, SpecificDatumReader())
            tweet1 = next(df)
            df.close()

        self.assertEqual(tweet.ID, tweet1.ID)
        self.assertEqual(tweet.text, tweet1.text)
        self.assertEqual(tweet.authorScreenName, tweet1.authorScreenName)
        self.assertEqual(tweet.authorProfileImageURL, tweet1.authorProfileImageURL)
        self.assertEqual(tweet.authorUserID, tweet1.authorUserID)

        self.assertTrue(isinstance(tweet1.location, AvroPoint))
        self.assertEqual(tweet.location.latitude, tweet1.location.latitude)
        self.assertEqual(tweet.location.longitude, tweet1.location.longitude)
        self.assertEqual(tweet.placeID, tweet1.placeID)
        self.assertTrue(isinstance(tweet1.createdAt, AvroDateTime))
        self.assertEqual(tweet.createdAt.dateTimeString, tweet1.createdAt.dateTimeString)

        self.assertTrue(isinstance(tweet1.metadata, AvroTweetMetadata))
        self.assertTrue(isinstance(tweet1.metadata.inReplyToScreenName, AvroKnowableOptionString))
        self.assertEqual(tweet.metadata.inReplyToScreenName.known, tweet1.metadata.inReplyToScreenName.known)
        self.assertEqual(tweet.metadata.inReplyToScreenName.data, tweet1.metadata.inReplyToScreenName.data)

        self.assertTrue(isinstance(tweet1.metadata.mentionedScreenNames, AvroKnowableListString))
        self.assertEqual(tweet.metadata.mentionedScreenNames.known, tweet1.metadata.mentionedScreenNames.known)
        self.assertEqual(tweet.metadata.mentionedScreenNames.data, tweet1.metadata.mentionedScreenNames.data)

        self.assertTrue(isinstance(tweet1.metadata.links, AvroKnowableListString))
        self.assertEqual(tweet.metadata.links.known, tweet1.metadata.links.known)
        self.assertEqual(tweet.metadata.links.data, tweet1.metadata.links.data)

        self.assertTrue(isinstance(tweet1.metadata.hashtags, AvroKnowableListString))
        self.assertEqual(tweet.metadata.hashtags.known, tweet1.metadata.hashtags.known)
        self.assertEqual(tweet.metadata.hashtags.data, tweet1.metadata.hashtags.data)

        self.assertTrue(isinstance(tweet1.metadata.isBareCheckin, AvroKnowableBoolean))
        self.assertEqual(tweet.metadata.isBareCheckin.known, tweet1.metadata.isBareCheckin.known)
        self.assertEqual(tweet.metadata.isBareCheckin.data, tweet1.metadata.isBareCheckin.data)

        self.assertTrue(isinstance(tweet1.metadata.isBareRetweet, AvroKnowableBoolean))
        self.assertEqual(tweet.metadata.isBareRetweet.known, tweet1.metadata.isBareRetweet.known)
        self.assertEqual(tweet.metadata.isBareRetweet.data, tweet1.metadata.isBareRetweet.data)

        self.assertTrue(isinstance(tweet1.metadata.isRetweet, AvroKnowableBoolean))
        self.assertEqual(tweet.metadata.isRetweet.known, tweet1.metadata.isRetweet.known)
        self.assertEqual(tweet.metadata.isRetweet.data, tweet1.metadata.isRetweet.data)

        self.assertTrue(isinstance(tweet1.metadata.venueID, AvroKnowableOptionString))
        self.assertEqual(tweet.metadata.venueID.known, tweet1.metadata.venueID.known)
        self.assertEqual(tweet.metadata.venueID.data, tweet1.metadata.venueID.data)

        self.assertTrue(isinstance(tweet1.metadata.venuePoint, AvroKnowableOptionPoint))
        self.assertEqual(tweet.metadata.venuePoint.known, tweet1.metadata.venuePoint.known)
        self.assertEqual(tweet.metadata.venuePoint.data, tweet1.metadata.venuePoint.data)

    @unittest.skip
    def test_defaults(self):
        schema_json = self.read_schema('record_with_default_nested.json')
        avrogen.schema.write_schema_files(schema_json, self.output_dir, use_logical_types=True)
        root_module, schema_classes = self.load_gen(self.test_name)

        self.assertTrue(hasattr(root_module, 'sample_recordClass'))
        record = root_module.sample_recordClass()

        self.assertEquals(record.withDefault.field1, 42)
        self.assertEquals(record.nullableWithDefault.field1, 42)
        self.assertEquals(record.nullableRecordWithLogicalType.field1, datetime.date(1970, 2, 12))
        self.assertEquals(record.nullableWithLogicalType, datetime.date(1970, 2, 12))
        self.assertEquals(record.multiNullable, 42)

    def primitive_type_tester(self, schema_name):
        schema_json = self.read_schema(schema_name)
        avrogen.schema.write_schema_files(schema_json, self.output_dir)
        root_module, schema_classes = self.load_gen(self.test_name)

        # self.assertTrue(hasattr(schema_classes, 'SchemaClasses'))


# if __name__ == "__main__":
#     unittest.main()
