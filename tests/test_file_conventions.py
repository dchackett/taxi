#!/usr/bin/env python

import unittest


from taxi.tasks import Task
from taxi.file import File, InputFile


class SimpleTester(object):
    def __init__(self, prefix, traj):
        self.prefix = prefix
        self.traj = traj        
    def to_dict(self):
        return self.__dict__
    
class RunnerTester(Task):    
    def __init__(self, prefix, traj, **kwargs):
        super(RunnerTester, self).__init__(**kwargs)
        self.prefix = prefix
        self.traj = traj
        
        
class TestFileConvention(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass
        
        
    def test_property_with_defaults_factory(self):
        # Don't alter defaults for other tests
        class LocalTester(SimpleTester):
            fout = File(conventions="{prefix}_{traj:d}")
        
        # Render using default
        a = LocalTester('a', 1)
        self.assertEqual(str(a.fout), 'a_1')
        
        # Change default in class
        LocalTester.fout.conventions = "{prefix}__{traj:d}"
        self.assertEqual(str(a.fout), 'a__1') # Instance renders differently when class-level default changed
        
        # Change for instance; default preserved
        b = LocalTester('a', 2)
        a.fout.conventions = "{prefix}___{traj:d}"
        c = LocalTester('a', 3)
        self.assertEqual(str(a.fout), "a___1") # Instance changed from default
        self.assertEqual(str(b.fout), "a__2") # Pre-existing instance still default
        self.assertEqual(str(c.fout), "a__3") # New instance still default
        
        # Change for default after instance is changed
        LocalTester.fout.conventions="{prefix}_{traj:d}"
        self.assertEqual(str(a.fout), "a___1") # Instance still has default overridden
        self.assertEqual(str(b.fout), "a_2") # Still using class-level default
        self.assertEqual(str(c.fout), "a_3") # Still using class-level default
        
        # Change another instance
        b.fout.conventions="{prefix}__{traj:d}"
        self.assertEqual(str(a.fout), "a___1") # Instance still has default overridden
        self.assertEqual(str(b.fout), "a__2") # Instance now has default overridden
        self.assertEqual(str(c.fout), "a_3") # Still using class-level default
        
        # Can "None out" fields
        a.fout = None
        self.assertIs(a.fout, None)
        
        
    def test_subclass_default_hierarchy(self):
        # Don't alter defaults for other tests
        class LocalTester(SimpleTester):
            fout = File(conventions="{prefix}_{traj:d}")
            
        class LocalTesterSubclass(LocalTester):
            pass
        
        class LocalTesterSubsubclass(LocalTesterSubclass):
            pass
        
        
        # Render using default
        a = LocalTester('a', 1)
        b = LocalTesterSubclass('b', 2)
        self.assertEqual(str(a.fout), 'a_1')
        self.assertEqual(str(b.fout), 'b_2')
        
        # Change default in class
        LocalTester.fout.conventions = "{prefix}__{traj:d}"
        self.assertEqual(str(a.fout), 'a__1') # Instance renders differently when class-level default changed
        self.assertEqual(str(b.fout), 'b__2') # Instance renders differently when class-level default changed
        
        # Change default in subclass
        LocalTesterSubclass.fout.conventions = "{prefix}___{traj:d}"
        self.assertEqual(str(a.fout), 'a__1') # Instance renders differently when class-level default changed
        self.assertEqual(str(b.fout), 'b___2') # Instance renders differently when class-level default changed
        
        # Change default in instance
        a.fout.conventions = "{prefix}_{traj:d}"
        b.fout.conventions = "{prefix}__{traj:d}"
        self.assertEqual(str(a.fout), 'a_1') # Instance renders differently when class-level default changed
        self.assertEqual(str(b.fout), 'b__2') # Instance renders differently when class-level default changed
        
        # Get default from overridden superclass(subclass) of subsubclass
        params = LocalTesterSubsubclass.fout.parse_params('c___3')
        self.assertEqual(params['prefix'], 'c')
        self.assertEqual(params['traj'], 3)


    def test_class_level_filename_parsing(self):
        # Don't alter defaults for other tests
        class LocalTester(SimpleTester):
            loadg = File(conventions="{prefix}_{traj:d}")
            
        # Parse filename
        params = LocalTester.loadg.parse_params("a_1")
        self.assertEqual(params['prefix'], 'a')
        self.assertEqual(params['traj'], 1)
        
        # Parse filename with extra path structure
        params = LocalTester.loadg.parse_params("path/to/a_1")
        self.assertEqual(params['prefix'], 'a')
        self.assertEqual(params['traj'], 1)
        
        # Parse filename with extra absolute path structure
        params = LocalTester.loadg.parse_params("/path/to/a_1")
        self.assertEqual(params['prefix'], 'a')
        self.assertEqual(params['traj'], 1)
        
        
    def test_instance_level_filename_parsing(self):
        # Don't alter defaults for other tests
        class LocalTester(SimpleTester):
            loadg = InputFile(conventions="{prefix}_{traj:d}")
        
        # Make instance, load in filename, should parse out automatically
        a = LocalTester('a', 1)
        a.loadg = "b_4"
        self.assertTrue(isinstance(a.loadg, basestring))
        self.assertEqual(a.prefix, 'b')
        self.assertEqual(a.traj, 4)
        
        # Load a second string, make sure it's 
        a = LocalTester('a', 1)
        a.loadg = "c_5"
        self.assertTrue(isinstance(a.loadg, basestring))
        self.assertEqual(a.prefix, 'c')
        self.assertEqual(a.traj, 5)
        
    def test_postprocessing(self):
        def _test_processor(params):
            params['c'] = str(params.pop('a')) + str(params.pop('b'))
            return params
            
        # Don't alter defaults for other tests
        class LocalTester(SimpleTester):
            loadg = InputFile(conventions="{a}_{b}", postprocessor=_test_processor)
            
        # Class-level parsing
        params = LocalTester.loadg.parse_params('as_df')
        self.assertEqual(params['c'], 'asdf')
        
        # Instance-level parsing
        a = LocalTester('b', '4')
        a.loadg = 'fd_sa'
        self.assertEqual(a.c, 'fdsa')        
        
        
#    def test_multiple_parsing_options(self):
#        # Don't alter defaults for other tests
#        class LocalTester(SimpleTester):
#            loadg = File(conventions=["{prefix}_{traj:d}", "{prefix}__{traj:d}"])
#            
#        # Class-level parsing
#        params = LocalTester.loadg.parse_params("a_1")
#        self.assertEqual(params['prefix'], 'a')
#        self.assertEqual(params['traj'], 1)
#        
#        params = LocalTester.loadg.parse_params("a__2")
#        self.assertEqual(params['prefix'], 'a')
#        self.assertEqual(params['traj'], 2)
#        
#        params = LocalTester.loadg.parse_params("a___3")
#        self.assertIs(params, None)
#            
#        # Instance-level parsing
#        a = LocalTester("b", 4)
#        
#        a.loadg = "a_1"
#        params = a.loadg.parse_params()
#        self.assertEqual(params['prefix'], 'a')
#        self.assertEqual(params['traj'], 1)
#        
#        a.loadg = "a__2"
#        params = a.loadg.parse_params()
#        self.assertEqual(params['prefix'], 'a')
#        self.assertEqual(params['traj'], 2)
#        
#        a.loadg = "a___3"
#        params = a.loadg.parse_params()
#        self.assertIs(params, None)







if __name__ == '__main__':
    suite1 = unittest.TestLoader().loadTestsFromTestCase(TestFileConvention)

    all_tests = unittest.TestSuite([suite1, ])
    unittest.TextTestRunner(verbosity=2).run(all_tests)
