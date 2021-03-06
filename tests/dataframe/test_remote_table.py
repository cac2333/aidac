import unittest
import re

import aidac
from aidac.dataframe.transforms import *


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        from tests.ds_config import PG_CONFIG
        config = PG_CONFIG
        aidac.add_data_source('postgres',
                              config['host'],
                              config['user'],
                              config['passwd'],
                              config['dbname'],
                              'p1',
                              config['port']
                              )
        aidac.add_data_source('postgres',
                              config['host'],
                              config['user'],
                              config['passwd'],
                              config['dbname'],
                              'p2',
                              config['port']
                              )
        self.station = aidac.read_remote_data('p1', 'couple')
        self.coup_ = aidac.read_remote_data("p2", "couple")
        # self.trip = aidac.read_remote_data('p1', 'tripdata2017')
        # self.station = aidac.read_remote_data('p1', 'stations2017')

    def test_remote_project1(self):
        proj1 = self.station[['couple_id', 'hcardid', 'sid']]
        self.assertTrue(isinstance(proj1.transform, SQLProjectionTransform))
        sql1 = proj1.transform.genSQL
        self.assertEqual(sql1,
                         'SELECT couple_id AS couple_id, hcardid AS hcardid, sid AS sid FROM (SELECT * FROM couple) couple')

        proj2 = proj1['sid']
        sql2 = proj2.transform.genSQL
        expected = 'SELECT sid AS sid FROM ({})'.format(sql1)
        self.assertRegex(sql2, re.escape(expected))

    def test_remote_join(self):
        jn = self.users.merge(self.review, 'userid', 'inner')
        sql = jn.transform.genSQL
        self.assertEqual(sql,
                         'SELECT users.userid AS userid_x, users.uname AS uname, users.email AS email, users.dateofbirth AS dateofbirth, '
                         'review.userid AS userid_y, review.movid AS movid, review.reviewdate AS reviewdate, review.rating AS rating, review.reviewtxt AS reviewtxt '
                         'FROM (SELECT * FROM users) users INNER JOIN (SELECT * FROM review) review ON users.userid = review.userid')
        jn.materialize()
        dd = jn._data_
        print(dd)

    def test_order_by(self):
        order_ = self.coup_.order("sid")
        self.assertTrue(isinstance(order_.transform, SQLOrderTransform))
        sql = order_.transform.genSQL
        self.assertEqual(sql, 'SELECT * FROM couple ORDER BY sid asc ')

    # def test_schdule1(self):
    #     proj = self.station['sid']
    #     proj.materialize()

    def test_group_by(self):
        group_by = self.coup_.groupby("sid", ["sid"])
        self.assertTrue(isinstance(group_by.transform, SQLGroupByTransform))
        sql = group_by.transform.genSQL
        self.assertEqual(sql, 'SELECT sid AS sid FROM (SELECT * FROM couple) couple GROUP BY sid')

    def test_fillna(self):
        fillna = self.coup_.fillna()
        self.assertTrue(isinstance(fillna.transform, SQLFillNA))
        sql = fillna.transform.genSQL
        self.assertEqual(sql,
                         "SELECT  coalesce( couple_id,'0')  AS couple_id, coalesce( hcardid,'0')  AS hcardid, coalesce( sid,'0')  AS sid FROM (SELECT * FROM couple) couple")
        #

    def test_dropduplicate(self):
        dd = self.coup_.drop_duplicates()
        self.assertTrue(isinstance(dd.transform, SQLDropduplicateTransform))
        print(f"current transform is {dd.transform}")
        sql = dd.transform.genSQL
        self.assertEqual(sql, 'SELECT DISTINCT couple_id, hcardid, sid FROM (SELECT * FROM couple) couple')

    def test_drop_na(self):
        dn = self.coup_.dropna()
        self.assertTrue(isinstance(dn.transform, SQLDropNA))
        sql = dn.transform.genSQL
        self.assertEqual(sql, "DELETE FROM couple where couple_id is NULL OR hcardid is NULL OR sid is NULL;")

        dn2 = self.coup_.dropna("couple_id")
        self.assertTrue(isinstance(dn.transform, SQLDropNA))
        sql_ = dn2.transform.genSQL
        self.assertEqual(sql_, "DELETE FROM couple where couple_id is NULL;")

    def test_query(self):
        q = self.coup_.query("couple_id BETWEEN 10 AND 100")
        self.assertTrue(isinstance(q.transform, SQLQuery))
        sql = q.transform.genSQL
        self.assertEqual(sql, "SELECT * FROM couple WHERE couple_id BETWEEN 10 AND 100")

        q2 = self.coup_.query("couple_id == 100 and sid != 0")
        self.assertTrue(isinstance(q2.transform, SQLQuery))
        sql2 = q2.transform.genSQL
        self.assertEqual(sql2, "SELECT * FROM couple WHERE couple_id = 100 and sid <> 0")
        q3 = q2.order("sid")
        sql3 = q3.transform.genSQL
        self.assertEqual(sql3, "SELECT * FROM couple WHERE couple_id = 100 and sid <> 0 ORDER BY sid asc ")

    def test_head(self):
        h = self.coup_.head(5)
        self.assertTrue(isinstance(h.transform, SQLHeadTransform))
        sql = h.transform.genSQL
        self.assertEqual(sql, 'SELECT * FROM couple LIMIT 5')

    def test_tail(self):
        t = self.coup_.tail(5)
        self.assertTrue(isinstance(t.transform, SQLTailTransform))
        sql = t.transform.genSQL
        self.assertEqual(sql, 'SELECT * FROM couple LIMIT 5 OFFSET (SELECT COUNT(*) FROM couple) - 5')

    def test_rename(self):
        r = self.coup_.rename({"couple_id": "cid"})
        self.assertTrue(isinstance(r.transform, SQLRenameTransform))
        sql = r.transform.genSQL
        self.assertEqual(sql, 'ALTER TABLE couple couple_id TO cid;')

    def test_aggregate(self):
        a = self.coup_.aggregate(["couple_id", "hcardid"], "hcardid")
        self.assertTrue(isinstance(a.transform, SQLAggregateTransform))
        sql = a.transform.genSQL
        self.assertEqual(sql, "SELECT couple_id AS couple_id, hcardid AS hcardid FROM (SELECT * FROM couple) couple GROUP BY hcardid")
if __name__ == '__main__':
    unittest.main()
