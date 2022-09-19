import json

import scrapy
from scrapy.http.request import Request

import execjs
# import pymysql.cursors
# from ..settings import MYSQL_INFO

# Connect to the database
# connection = pymysql.connect(host=MYSQL_INFO['host'], port=MYSQL_INFO['port'],
#                              user=MYSQL_INFO['user'], password=MYSQL_INFO['password'],
#                              database=MYSQL_INFO['schema'],
#                              cursorclass=pymysql.cursors.DictCursor)


# def exec_sql(sql, *args):
#     with connection.cursor() as cursor:
#         cursor.execute(sql, args)
#         connection.commit()
#
#         return cursor.fetchall()


class BondFundRankSpider(scrapy.Spider):
    name = 'BondFundRank'
    allowed_domains = ['fund.eastmoney.com']
    start_urls = ['http://fund.eastmoney.com/']
    mark = ['y', '3y', '6y']
    weight = {
        'y': 40,
        '3y': 40,
        '6y': 20,
    }
    bond_kind = {
        "长期纯债": '041',
        "短期纯债": '042'
    }

    url_template = 'https://fundapi.eastmoney.com/fundtradenew.aspx?ft=zq&sc={}&st=desc&pi=1&' \
                   'pn=100&cp=&ct=&cd=&ms=&fr={}&plevel=&fst=&ftype=&fr1=&fl=0&isab=1'

    sql_clear = 'delete from BondFundRank'
    sql_template = 'insert into BondFundRank(code, name, rank_date,' \
                   'recent_1week, recent_1month, recent_3month, recent_6month, recent_1year, recent_2year, ' + \
                   'recent_3year, current_year) values("{}", "{}", "{}", {}, {}, {}, {}, {}, {}, {}, {}) ' + \
                   'on duplicate key update recent_1week={}, recent_1month={}, recent_3month={}, recent_6month={}, ' + \
                   'recent_1year={}, recent_2year={}, recent_3year={}, current_year={}'

    def __init__(self, name=None, **kwargs):
        super().__init__(name, **kwargs)
        self.finished = 0
        self.score = {}

    def start_requests(self):
        # for mark in self.mark:
        #     yield Request(self.url_template.format(mark, self.bond_kind['长期纯债']), dont_filter=True,
        #                   meta={"mark": mark, 'period': "长期纯债"})

        self.weight = {
            'y': 40,
            '3y': 40,
            '6y': 20,
        }
        for mark in self.mark:
            yield Request(self.url_template.format(mark, self.bond_kind['短期纯债']), dont_filter=True,
                          meta={"mark": mark, 'period': '短期纯债'})

    def parse(self, response, **kwargs):
        code = response.body.decode()
        ctx = execjs.compile(code)
        data = ctx.eval('rankData')['datas']
        with open('{}_{}.csv'.format(response.meta['period'], response.meta['mark']), 'w', encoding='utf-8') as f:
            rank_score = len(data)
            for u in data:
                f.write(u + '\n')
                detail = u.split('|')

                if detail[1] not in self.score:
                    self.score[detail[1]] = [detail[0], rank_score * self.weight[response.meta['mark']] / 100.0]
                else:
                    self.score[detail[1]][1] += rank_score * self.weight[response.meta['mark']] / 100.0

                rank_score -= 1

                # for i in range(14):  # 填充空白数据
                #     if detail[i]:
                #         continue
                #
                #     detail[i] = 0

                # code, name, fund_type, date, net_value, daily_grow, recent_1week, recent_1month, recent_3month, \
                # recent_6month, recent_1year, recent_2year, recent_3year, current_year = detail[:14]
                #
                # if self.reset is False:
                #     exec_sql(self.sql_clear)
                #     self.reset = True
                #
                # sql = self.sql_template.format(code, name, date, recent_1week, recent_1month, recent_3month,
                # recent_6month, recent_1year, recent_2year, recent_3year, current_year, recent_1week, recent_1month,
                # recent_3month, recent_6month, recent_1year, recent_2year, recent_3year, current_year)
                #
                # exec_sql(sql)

        if self.finished == len(self.mark)-1:
            flatten_list = []
            for name, detail in self.score.items():
                flatten_list.append((name, detail[0], detail[1]))

            flatten_list.sort(key=lambda elem: elem[2], reverse=True)

            with open('加权得分_{}.txt'.format(response.meta['period']), 'w', encoding='utf-8') as f:
                for e in flatten_list:
                    f.write(json.dumps(e, ensure_ascii=False) + '\n')
        else:
            self.finished += 1
