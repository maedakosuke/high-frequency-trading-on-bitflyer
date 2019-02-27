# -*- coding: utf-8 -*-
"""
Created on Fri Feb 22 21:11:00 2019

@author: magimagi1223

https://note.mu/magimagi1223/n/n34181710810e
"""

import requests

# 3/1-3/31のohlcv(1時間足)を取得
timestamp = '1519830000'
r = requests.get(
    'https://www.bitmex.com/api/udf/history?symbol=XBTUSD&resolution=60&from='
    + timestamp + '&to=' + str(int(timestamp) + 60 * 60 * 24 * 31))
ohlcv = r.json()


def eval_backtest(individual):
    # 個体から遺伝子を取り出しパラメータとして代入
    S = individual[0]
    L = individual[1]
    X = individual[2]
    Y = individual[3]
    Z = individual[4]

    last = 0
    sma_s = 0
    sma_l = 0
    list_last = []
    list_rate = []
    list_sma_s = []
    list_sma_l = []
    rate_max = 0
    rate_min = 99999999
    margin = 0
    total_prof = 0
    total_loss = 0
    count_prof = 0
    count_loss = 0
    pos = 'none'

    # 終値とSMAを計算
    for i in range(len(ohlcv['c']) - 1):
        last = ohlcv['c'][i]
        list_last.append(last)

        if len(list_last) > S:
            sma_s = sum(list_last[i - 1 - S:i - 1]) / S
        if len(list_last) > L:
            sma_l = sum(list_last[i - 1 - L:i - 1]) / L

            list_rate.append(last)
            list_sma_s.append(sma_s)
            list_sma_l.append(sma_l)

    # バックテスト実行
    for i in range(1, len(list_rate)):
        rate = list_rate[i]
        sma_s = list_sma_s[i]
        sma_l = list_sma_l[i]
        prev_sma_s = list_sma_s[i - 1]
        prev_sma_l = list_sma_l[i - 1]

        # ゴールデンクロスでロングエントリ
        if pos == 'none' and sma_s > sma_l and prev_sma_s < prev_sma_l:
            rate_entry = rate
            pos = 'entry_long'

        # デッドクロスでショートエントリ
        if pos == 'none' and sma_l > sma_s and prev_sma_l < prev_sma_s:
            rate_entry = rate
            pos = 'entry_short'

        # ロングの利確/損切
        if pos == 'entry_long':
            rate_max = max(rate_max, rate)

            if rate > rate_entry + X or rate < rate_max - Z:
                fee = (rate_entry + rate) * 0.00075  # taker手数料0.075%
                margin = rate - rate_entry - fee
                pos = 'exit'

        # ショートの利確/損切
        if pos == 'entry_short':
            rate_min = min(rate_min, rate)

            if rate < rate_entry - Y or rate > rate_min + Z:
                fee = (rate_entry + rate) * 0.00075  # taker手数料0.075%
                margin = rate_entry - rate - fee
                pos = 'exit'

        # 損益計算
        if pos == 'exit':
            if margin >= 0:
                total_prof += margin
                count_prof += 1
            else:
                total_loss += margin
                count_loss += 1

            # 後処理
            rate_max = 0
            rate_min = 99999999
            pos = 'none'

    # テスト結果
    pal = float(total_prof + total_loss)  # リターン Profit and Loss
    pf = float(total_prof / (-total_loss))  # プロフィットファクター
    wp = float(count_prof / (count_prof + count_loss))  # 勝率

    # 適応度にする指標を返り値として設定
    return pal,


# --------------------------------------------------------------------

import random
from deap import base
from deap import creator
from deap import tools

# パラメータの候補値リストを定義
list_S = [3, 4, 5, 6, 12, 13]
list_L = [24, 25, 26, 30, 48]

X = 5
Y = 5
Z = 5
list_X = []
list_Y = []
list_Z = []
while X <= 500:
    list_X.append(X)
    X += 5
while Y <= 500:
    list_Y.append(Y)
    Y += 5
while Z <= 500:
    list_Z.append(Z)
    Z += 5


# パラメータ値をランダムに決定する関数（個体生成関数の源泉）
def shuffle(container):
    params = [list_S, list_L, list_X, list_Y, list_Z]
    shuffled = []
    for x in params:
        shuffled.append(random.choice(x))
    return container(shuffled)


# list内のパラメータ値をランダムに変更する関数（突然変異関数の源泉）
def mutShuffle(individual, indpb):
    params = [list_S, list_L, list_X, list_Y, list_Z]
    for i in range(len(individual)):
        if random.random() < indpb:
            individual[i] = random.choice(params[i])
    return individual,


# 適合度クラスを作成
creator.create("FitnessMax", base.Fitness, weights=(1.0, ))
creator.create("Individual", list, fitness=creator.FitnessMax)

toolbox = base.Toolbox()

# 個体生成関数,世代生成関数を定義
toolbox.register("individual", shuffle, creator.Individual)
toolbox.register("population", tools.initRepeat, list, toolbox.individual)

# 評価関数,交叉関数,突然変異関数,選択関数を定義
toolbox.register("evaluate", eval_backtest)
toolbox.register("mate", tools.cxTwoPoint)
toolbox.register("mutate", mutShuffle, indpb=0.05)
toolbox.register("select", tools.selTournament, tournsize=3)

# --------------------------------------------------------------------


# メイン処理
def main():
    # random.seed(1024)

    # 個体をランダムにn個生成し、初期世代を生成
    pop = toolbox.population(n=100)  # n:世代の個体数
    CXPB, MUTPB, NGEN = 0.5, 0.2, 40  # 交叉確率、突然変異確率、ループ回数

    print("Start of evolution")

    # 初期世代の全個体の適応度を目的関数により評価
    fitnesses = list(map(toolbox.evaluate, pop))
    for ind, fit in zip(pop, fitnesses):
        ind.fitness.values = fit

    print("  Evaluated %i individuals" % len(pop))

    # ループ開始
    for g in range(NGEN):
        print("-- Generation %i --" % g)

        # 現行世代から個体を選択し次世代に追加
        offspring = toolbox.select(pop, len(pop))
        offspring = list(map(toolbox.clone, offspring))

        # 選択した個体に交叉を適応
        for child1, child2 in zip(offspring[::2], offspring[1::2]):
            if random.random() < CXPB:
                toolbox.mate(child1, child2)
                del child1.fitness.values
                del child2.fitness.values

        # 選択した個体に突然変異を適応
        for mutant in offspring:
            if random.random() < MUTPB:
                toolbox.mutate(mutant)
                del mutant.fitness.values

        # 適応度が計算されていない個体を集めて適応度を計算
        invalid_ind = [ind for ind in offspring if not ind.fitness.valid]
        fitnesses = map(toolbox.evaluate, invalid_ind)
        for ind, fit in zip(invalid_ind, fitnesses):
            ind.fitness.values = fit

        print("  Evaluated %i individuals" % len(invalid_ind))

        # 次世代を現行世代にコピー
        pop[:] = offspring

        # 全個体の適応度をlistに格納
        fits = [ind.fitness.values[0] for ind in pop]

        # 適応度の最大値、最小値、平均値、標準偏差を計算
        length = len(pop)
        mean = sum(fits) / length
        sum2 = sum(x * x for x in fits)
        std = abs(sum2 / length - mean**2)**0.5

        print("  Min %s" % min(fits))
        print("  Max %s" % max(fits))
        print("  Avg %s" % mean)
        print("  Std %s" % std)

    print("-- End of (successful) evolution --")

    # 最後の世代の中で最も適応度の高い個体のもつパラメータを準最適解として出力
    best_ind = tools.selBest(pop, 1)[0]
    print("Best parameter is %s, %s" % (best_ind, best_ind.fitness.values))


# --------------------------------------------------------------------

# 実行
if __name__ == "__main__":
    main()
