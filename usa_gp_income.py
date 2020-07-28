#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys, getopt

# 富途
zqCom = "futu"


# 佣金
# 美股0.0049美元，每笔订单最低0.99美元
def yongJin(num):
    calResult = 0.0049 * num
    return calResult if calResult > 0.99 else 0.99


# 平台使用费
# 固定模式，美股0.005美元，每笔订单最低1美元
def platform(num):
    calResult = 0.005 * num
    return calResult if calResult > 1 else 1


# 代收费
# 交收费，0.003美元*成交股数
def daiShou(num):
    calResult = 0.003 * num
    return calResult


# 卖出时收取
# 证监会规费，0.0000221美元*交易金额，最低0.01美元
def jianZheng(amount):
    calResult = 0.0000221 * amount
    return calResult if calResult > 0.01 else 0.01


# 交易活动费，0.000119美元*卖出数量，最低0.01美元，最高5.95美元
def trade(num):
    calResult = 0.000119 * num
    if calResult < 0.01:
        return 0.01
    elif calResult > 5.95:
        return 5.95
    else:
        return calResult


def sellInfo(price, num, total, buyFee, rate):
    sellPrice = price * (1 + rate)
    sellFee = yongJin(num) + platform(num) + daiShou(num) + jianZheng(sellPrice * num) + trade(num)
    shouRu = (sellPrice - price) * num - sellFee - buyFee
    msg = '涨幅: %s, 卖出价格: %s, 卖出费用: %s, 总费用: %s, 收入: %s' % (str(round(rate * 100, 2))+'%', "%.2f" % sellPrice, round(sellFee, 1), round(buyFee + sellFee, 1), round(shouRu, 2))
    return msg


def tradeInfo(price, num, rate):
    total = price * num
    buyFee = yongJin(num) + platform(num) + daiShou(num)
    msg = '''
----------------- info -----------------
买入价格: %s
买入数量: %s
投入金额: %s
买入费用: %s
''' % (price, num, total, round(buyFee, 1))
    print(msg)
    if rate == 0:
        for n in range(1, 10):
            print(sellInfo(price, num, total, buyFee, 0.01 * n))
    else:
        print(sellInfo(price, num, total, buyFee, rate))
    print('----------------- end ------------------')


# 计算
def main(argv):
    price = 0.0
    num = 0
    rate = 0.0
    try:
        opts, args = getopt.getopt(argv, "hp:n:r:", ["pArg=", "nArg="])
    except getopt.GetoptError:
        print('usa_gp_income.py -p <买入价格> -n <买入数量> -r <上涨几个点>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('usa_gp_income.py -p <买入价格> -n <买入数量> -r <上涨几个点>')
            sys.exit()
        elif opt in ("-p", "--pArg"):
            price = float(arg)
        elif opt in ("-n", "--nArg"):
            num = int(arg)
        elif opt in ("-r"):
            rate = float(arg)
    tradeInfo(price, num, rate)


if __name__ == "__main__":
    main(sys.argv[1:])
