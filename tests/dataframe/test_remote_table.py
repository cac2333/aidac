import unittest
import re

import aidac

from aidac.dataframe.transforms import *

import datetime


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
        self.midwife_ = aidac.read_remote_data("p1", "midwife")
        self.users = aidac.read_remote_data('p1', 'users')
        self.review = aidac.read_remote_data('p1', 'review')
        self.info_session_ = aidac.read_remote_data("p1", "info_session")

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

    def test_numerical_operation(self):
        ad = self.midwife_['prac_id'] + 1
        mul = self.midwife_['prac_id'] * ad
        sql = mul.genSQL
        self.assertRegex(sql, r'SELECT \(\(1\+prac_id\)\*prac_id\) AS prac_id FROM \(SELECT prac_id AS prac_id FROM'
                    ' \(SELECT \* FROM midwife\) midwife\) SQLProjectionTransform\d')

    def test_set_item1(self):
        self.midwife_['new_id'] = (self.midwife_['prac_id'] + 1 ) * self.midwife_['prac_id']
        self.assertCountEqual(self.midwife_.columns.keys(), ['prac_id', 'new_id', 'name', 'phone', 'email', 'iid'])
        sql = self.midwife_.genSQL
        self.assertEqual(sql,     'SELECT prac_id AS prac_id, email AS email, name AS name, '
                                'phone AS phone, iid AS iid, (prac_id*(1+prac_id)) AS new_id '
                                  'FROM (SELECT * FROM midwife) midwife')

        self.assertEqual(len(self.midwife_._frame_stubs), 2)
        self.assertTrue(self.midwife_._frame_stubs[0].transform is None)
        self.assertTrue(isinstance(self.midwife_._frame_stubs[1].transform, SQLProjectionTransform))

    def test_set_item2(self):
        self.midwife_[['const_id1', 'const_id2']] = [1, 'str1']
        self.assertCountEqual(self.midwife_.columns.keys(), ['prac_id', 'const_id1','const_id2', 'name', 'phone', 'email', 'iid'])
        sql = self.midwife_.genSQL
        self.assertEqual(sql,     'SELECT prac_id AS prac_id, email AS email, name AS name, '
                                'phone AS phone, iid AS iid, 1 AS const_id1, \'str1\' AS const_id2 '
                                  'FROM (SELECT * FROM midwife) midwife')
        self.midwife_.materialize()

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
        order_ = self.coup_.sort_values("sid")
        self.assertTrue(isinstance(order_.transform, SQLOrderTransform))
        sql = order_.transform.genSQL
        self.assertEqual(sql, 'SELECT * FROM couple ORDER BY sid asc')


    def test_group_by(self):
        group_by = self.coup_.groupby("sid")

        self.assertTrue(isinstance(group_by.transform, SQLGroupByTransform))
        sql = group_by.transform.genSQL
        self.assertEqual(sql, 'SELECT sid AS sid FROM (SELECT * FROM couple) couple GROUP BY sid ORDER BY sid')
        gb = self.midwife_.groupby(["iid", "email"])
        self.assertTrue(isinstance(gb.transform, SQLGroupByTransform))
        sql = gb.transform.genSQL
        self.assertEqual(sql,
                         "SELECT email AS email, iid AS iid FROM (SELECT * FROM midwife) midwife GROUP BY iid, email ORDER BY iid, email")

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

    def test_filter(self):
        fl = self.coup_[self.coup_['couple_id']>10]
        self.assertCountEqual(fl.columns.keys(), self.coup_.columns.keys())
        sql1 = fl.genSQL
        print(sql1)

        fl2 = self.coup_[(self.coup_['couple_id']>10) & (self.coup_['couple_id']<100)]
        sql2 = fl2.genSQL
        print(sql2)


    def test_eq(self):
        eq = self.midwife_ == 6
        self.assertTrue(isinstance(eq.transform, SQLFilterTransform))
        sql = eq.transform.genSQL
        self.assertEqual(sql, "SELECT CASE WHEN prac_id = 6 THEN TRUE ELSE FALSE END AS prac_id,"
                              " CASE WHEN 1 <> 1 THEN TRUE ELSE FALSE END AS email, CASE WHEN "
                              "1 <> 1 THEN TRUE ELSE FALSE END AS name, CASE WHEN phone = 6 THEN"
                              " TRUE ELSE FALSE END AS phone, CASE WHEN iid = 6 THEN TRUE ELSE F"
                              "ALSE END AS iid FROM (SELECT * FROM midwife) midwife")

    def test_ne(self):
        ne = self.midwife_ != 6
        self.assertTrue(isinstance(ne.transform, SQLFilterTransform))
        sql = ne.transform.genSQL
        self.assertEqual(sql, "SELECT CASE WHEN prac_id <> 6 THEN TRUE ELSE FALSE END AS prac_id,"
                              " CASE WHEN 1 = 1 THEN TRUE ELSE FALSE END AS email, CASE WHEN 1"
                              " = 1 THEN TRUE ELSE FALSE END AS name, CASE WHEN phone <> 6 THEN "
                              "TRUE ELSE FALSE END AS phone, CASE WHEN iid <> 6 THEN TRUE ELSE FA"
                              "LSE END AS iid FROM (SELECT * FROM midwife) midwife")

    def test_gt(self):
        gt = self.midwife_ > 6
        self.assertTrue(isinstance(gt.transform, SQLFilterTransform))
        sql = gt.transform.genSQL
        self.assertEqual(sql, "SELECT CASE WHEN prac_id > 6 THEN TRUE ELSE FALSE END AS prac_id,"
                              " CASE WHEN 1 <> 1 THEN TRUE ELSE FALSE END AS email, CASE WHEN 1"
                              " <> 1 THEN TRUE ELSE FALSE END AS name, CASE WHEN phone > 6 THEN "
                              "TRUE ELSE FALSE END AS phone, CASE WHEN iid > 6 THEN TRUE ELSE FA"
                              "LSE END AS iid FROM (SELECT * FROM midwife) midwife")

    def test_ge(self):
        ge = self.midwife_ >= 6
        self.assertTrue(isinstance(ge.transform, SQLFilterTransform))
        sql = ge.transform.genSQL
        self.assertEqual(sql, "SELECT CASE WHEN prac_id >= 6 THEN TRUE ELSE FALSE END AS prac_id,"
                              " CASE WHEN 1 <> 1 THEN TRUE ELSE FALSE END AS email, CASE WHEN 1"
                              " <> 1 THEN TRUE ELSE FALSE END AS name, CASE WHEN phone >= 6 THEN "
                              "TRUE ELSE FALSE END AS phone, CASE WHEN iid >= 6 THEN TRUE ELSE FA"
                              "LSE END AS iid FROM (SELECT * FROM midwife) midwife")

    def test_le(self):
        le = self.midwife_ <= 6
        self.assertTrue(isinstance(le.transform, SQLFilterTransform))
        sql = le.transform.genSQL
        self.assertEqual(sql, "SELECT CASE WHEN prac_id <= 6 THEN TRUE ELSE FALSE END AS prac_id,"
                              " CASE WHEN 1 <> 1 THEN TRUE ELSE FALSE END AS email, CASE WHEN 1"
                              " <> 1 THEN TRUE ELSE FALSE END AS name, CASE WHEN phone <= 6 THEN "
                              "TRUE ELSE FALSE END AS phone, CASE WHEN iid <= 6 THEN TRUE ELSE FA"
                              "LSE END AS iid FROM (SELECT * FROM midwife) midwife")

    def test_agg(self):
        ag = self.midwife_.agg("count", ["name", "iid"])
        self.assertTrue(isinstance(ag.transform, SQLAGG_Transform))
        sql = ag.transform.genSQL
        self.assertEqual(sql, "SELECT count(name) AS count_name, count(iid) AS count_iid FROM (SELECT * FROM midwife) midwife")

    def test_lt(self):
        ge = self.midwife_ < 6
        self.assertTrue(isinstance(ge.transform, SQLFilterTransform))
        sql = ge.transform.genSQL
        self.assertEqual(sql, "SELECT CASE WHEN prac_id < 6 THEN TRUE ELSE FALSE END AS prac_id,"
                              " CASE WHEN 1 <> 1 THEN TRUE ELSE FALSE END AS email, CASE WHEN 1"
                              " <> 1 THEN TRUE ELSE FALSE END AS name, CASE WHEN phone < 6 THEN "
                              "TRUE ELSE FALSE END AS phone, CASE WHEN iid < 6 THEN TRUE ELSE FA"
                              "LSE END AS iid FROM (SELECT * FROM midwife) midwife")

    def test_group_agg(self):
        gag = self.midwife_.groupby("iid").agg(func="count")
        # p = gag.columns

        sql = gag.transform.genSQL
        self.assertEqual(sql, "SELECT count(prac_id) AS prac_id, count(email) AS email,"
                              " count(name) AS name, count(phone) AS phone, count(iid) "
                              "AS iid FROM (SELECT * FROM midwife)midwife GROUP BY iid ORDER "
                              "BY iid")
        #
        gag2 = self.midwife_.groupby("iid").agg(collist=
                                                {"prac_id": ["count", "max"],
                                                 "email": "count",
                                                 "iid": ["count", "avg"]})
        sql2 = gag2.transform.genSQL
        self.assertEqual(sql2,"SELECT count(prac_id) AS count_prac_id, max(prac_id) AS max_prac_id,"
                              " count(email) AS email, count(iid) AS count_iid, avg(iid) AS avg_iid, "
                              "iid AS iid FROM (SELECT * FROM midwife)midwife GROUP BY iid ORDER BY iid")

        gag2 = self.midwife_.groupby("iid").agg(collist=
                                                {"prac_id": ["count", "max"],
                                                 "email": "count",
                       "iid": ["count", "avg"]})

    def test_contains(self):

        ct = self.midwife_[self.midwife_["email"].contains(".com")]
        sql = ct.transform.genSQL

        self.assertEqual(sql, "")

    def test_materialize(self):
        # mt = self.info_session_.materialize()
        is_ = self.info_session_ > datetime.date(1999, 1, 1)

        sql = is_.genSQL
        self.assertEqual(sql, "")

    def test_date(self):
        is_ = self.info_session_ > datetime.date(1999, 1, 1)
        sql = is_.genSQL
        self.assertEqual(sql, "SELECT CASE WHEN 1 <> 1 THEN TRUE ELSE FALSE END AS session_id, "
                              "CASE WHEN date > '1999-01-01' THEN TRUE ELSE FALSE END AS date, "
                              "CASE WHEN 1 <> 1 THEN TRUE ELSE FALSE END AS language, CASE WHEN"
                              " 1 <> 1 THEN TRUE ELSE FALSE END AS prac_id FROM (SELECT * FROM "
                              "info_session) info_session")

    def test_yrs(self):
        is_ = self.info_session_ > datetime.date(1999, 1, 1).year
        sql = is_.genSQL
        self.assertEqual(sql, "") # should be error

    def test_contains_2(self):

        mw = self.midwife_
        mw_series = mw["email"].str

        mw_sql = mw_series.genSQL

        self.assertEqual(mw_sql, "SELECT email AS email FROM (SELECT * FROM midwife) midwife")

        mw_contains_1 = mw_series.contains("w", regex=True, case=True)
        mw_contains_2 = mw_series.contains("WFW", regex=True, case=False)
        mw_contains_3 = mw_series.contains("WW", regex = False, case = True)
        mw_contains_4 = mw_series.contains("eWFW", regex=False, case=False)

        mw_sql1 = mw_contains_1.genSQL
        mw_sql2 = mw_contains_2.genSQL
        mw_sql3 = mw_contains_3.genSQL
        mw_sql4 = mw_contains_4.genSQL

        self.assertEqual(mw_sql1, "SELECT * FROM (SELECT email AS email FROM (SELECT * FROM midwife"
                                  ") midwife) SQLProjectionTransform8 WHERE email LIKE '%w%'")

        self.assertEqual(mw_sql2, "SELECT * FROM (SELECT email AS email FROM (SELECT * FROM midwife"
                                  ") midwife) SQLProjectionTransform8 WHERE LOWER(email) LIKE '%wfw"
                                  "%'")

        self.assertEqual(mw_sql3, "SELECT * FROM (SELECT email AS email FROM (SELECT * FROM midwife"
                                  ") midwife) SQLProjectionTransform8 WHERE email LIKE '%WW%'")

        self.assertEqual(mw_sql4, "SELECT * FROM (SELECT email AS email FROM (SELECT * FROM midwife"
                                  ") midwife) SQLProjectionTransform8 WHERE LOWER(email) LIKE '%ewfw%'")



if __name__ == '__main__':
    unittest.main()
