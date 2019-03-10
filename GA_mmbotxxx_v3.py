# -*- coding: utf-8 -*-
"""
Created on Sun Mar 10 09:45:49 2019

@author: kosuke

darden_ga.pyをベースに作成
"""

import random
#import numpy as np
from operator import attrgetter
import Strategy

def main():
    NIND    = 100   # The number of individuals in a population.
    CXPB     = 0.5   # The probability of crossover.
    MUTPB    = 0.2   # The probability of individdual mutation.
    MUTINDPB = 0.05  # The probability of gene mutation.
    NGEN     = 10    # The number of generation loop.

    random.seed(64)
    # --- Step1 : Create initial generation.
    print("Create initial generation.")
    pop = create_pop(NIND)
    set_fitness(obj_func, pop)
    best_ind = max(pop, key=attrgetter("fitness"))

    # --- Generation loop.
    print("Generation loop start.")
    print("Generation: 0. Best fitness: " + str(best_ind.fitness))
    for g in range(NGEN):

        # --- Step2 : Selection.
        offspring = selTournament(pop, NIND, tnsize=3)

        # --- Step3 : Crossover.
        crossover = []
        for child1, child2 in zip(offspring[::2], offspring[1::2]):
            if random.random() < CXPB:
                child1, child2 = cxTwoPointCopy(child1, child2)
                #child1.fitness = None
                #child2.fitness = None
            crossover.append(child1)
            crossover.append(child2)

        offspring = crossover[:]

        # --- Step4 : Mutation.
        mutant = []
        for mut in offspring:
            if random.random() < MUTPB:
                mut = mutFlipBit(mut, indpb=MUTINDPB)
                #mut.fitness = None
            mutant.append(mut)

        offspring = mutant[:]

        # --- Update next population.
        pop = offspring[:]
        set_fitness(obj_func, pop)

        # --- Print best fitness in the population.
        best_ind = max(pop, key=attrgetter("fitness"))
        print("Generation: " + str(g+1) + ". Best fitness: " + str(best_ind.fitness))

    print("Generation loop ended. The best individual: ")
    print(best_ind)


class Individual(dict):
    """Container of a individual."""
    fitness = None
    def __new__(cls, d):
        return super().__new__(cls, d)


def make_rnd_param(paramname):
    if paramname == 'order_size':
        #return random.randrange(1, 10, 1) / 100
        return 0.01
    elif paramname == 'integral_time':
        return random.randrange(5, 60, 5)
    elif paramname == 'filter_low':
        return random.randrange(0, 5, 1)
    elif paramname == 'filter_high':
        return random.randrange(6, 10, 1)
    elif paramname == 'profit_spread':
        return random.randrange(-100, 0, 10)
    elif paramname == 'close_time':
        return random.randrange(60, 600, 60)


def create_ind():
    """Create a individual."""
    params = {
        'order_size': None,  # 注文サイズ [BTC]
        'integral_time': None,  # 投資指標(約定履歴)の積算時間 [s]
        'filter_low': None,  # 注文判定に使用する投資指標の閾値Low [BTC]^0.5
        'filter_high': None,  # 閾値High [BTC]^0.5
        'profit_spread': None,  # 注文金額のbest bid/askからの差 [JPY]
        'close_time': None,  # closetime秒後に反対取引をしてポジションを精算する [s]
    }
    for paramname in params:
        params['%s'%paramname] = make_rnd_param(paramname)

    return Individual(params)


def create_pop(n_ind):
    """Create a population."""
    pop = []
    for i in range(n_ind):
        ind = create_ind()
        pop.append(ind)
    return pop


def set_fitness(obj_func, pop):
    """Set fitnesses of each individual in a population."""
    for i, fit in zip(range(len(pop)), map(obj_func, pop)):
        pop[i].fitness = fit


def obj_func(ind):
    """Objective function."""
    # indはdict + fitness
    # pd.Series(ind)でdictのみ残りfitnessは消える
    # 目的関数のreturnがfitnessなのでOK
    return Strategy.total_asset(ind)


def selTournament(pop, nind, tnsize):
    """Selection function."""
    chosen = []
    for i in range(nind):
        aspirants = [random.choice(pop) for j in range(tnsize)]
        chosen.append(max(aspirants, key=attrgetter("fitness")))
    return chosen


def cxTwoPointCopy(ind1, ind2):
    """Crossover function."""
    size = len(ind1)
    # 交叉点を決定する
    cxpoint1 = random.randint(0, size-1)
    cxpoint2 = random.randint(0, size-2)
    if cxpoint2 >= cxpoint1:
        cxpoint2 += 1
    else:
        cxpoint1, cxpoint2 = cxpoint2, cxpoint1
    # 交叉する
    #print('crossover', cxpoint1, cxpoint2)
    cp1 = ind1.copy()
    cp2 = ind2.copy()
    for idx, elem in enumerate(ind1):
        if idx >= cxpoint1 and idx <= cxpoint2:
            cp1['%s'%elem] = ind2['%s'%elem]
            cp2['%s'%elem] = ind1['%s'%elem]

    return Individual(cp1), Individual(cp2)


def mutFlipBit(ind, indpb):
    """Mutation function."""
    tmp = ind.copy()
    for paramname in tmp:
        if random.random() < indpb:
            tmp['%s'%paramname] = make_rnd_param(paramname)

    return Individual(tmp)


if __name__ == "__main__":
    main()