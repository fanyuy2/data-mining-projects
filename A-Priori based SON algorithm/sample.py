import son_apriori
from pyspark import SparkContext
import sys, time, math, itertools, collections, random


if __name__ == "__main__":
    # measure time 
    start = time.time()
    # print("Program started:", start)

    filter_threshold = int(sys.argv[1])
    support = int(sys.argv[2])
    input_file_path = sys.argv[3]
    output_file_path = sys.argv[4]


    sc = SparkContext("local" , "task2")
    # sc.setLogLevel("ERROR") 


    # load data
    input_RDD = sc.textFile(input_file_path) \
        .filter(lambda x: not x.startswith('"TRANSACTION_DT"')) \
        .map(lambda x: x.split(',')) \
        .map(lambda x: [(x[0][1:-5]+x[0][-3:-1]+"-"+str(int(x[1][1:-1]))), str(int(x[5][1:-1]))])
    

    #Output preprocess result
    processed_data = "DATE-CUSTOMER_ID,PRODUCT_ID\n"
    input_list = input_RDD.collect()
    for i in input_list:
        new_line = str(i[0]) + ',' + str(i[1]) + '\n'
        processed_data += new_line
    with open('./publicdata/processed_ta_feng.csv', 'w+') as f:
        f.write(processed_data)



    # print("preprocessed data:", input_RDD.collect())
    n_partitions = 2
    baskets_RDD = input_RDD.map(lambda x: (x[0], x[1])) \
        .groupByKey() \
        .mapValues(set) \
        .map(lambda x: (x[0], list(x[1]))) \
        .filter(lambda x: len(x[1]) > filter_threshold) 
    
    total_baskets = baskets_RDD.count()
    
    #)
    baskets_RDD = baskets_RDD.partitionBy(n_partitions, son_apriori.partition)


    # print(baskets_RDD.getNumPartitions())

    # print("baskets:", baskets_RDD.collect())

    itemsets_RDD = baskets_RDD.values().cache()
    # a = items.collect()

    # print(baskets_RDD.collect())

     # SON algorithm pass 1. 
    # print("Son pass 1 started:", time.time())

    # son pass 1
    def son_1(sub_baskets):
        
        #print(BarrierTaskContext.partitionId())
        sub_baskets = list(sub_baskets)
        # calculate local threshold. 
        local_support = math.ceil(support * len(sub_baskets)/ total_baskets)
   
        # map: find local frequent itemsets usinig a-priori algorithm. 
        result_ls = son_apriori.a_priori(sub_baskets, local_support)

        return result_ls
    
    candidates_RDD = itemsets_RDD.mapPartitions(lambda x: son_1(x)).distinct()
    candidates_ls = candidates_RDD.collect()

    """rm_duplicate_ls = []
    for itemsets in candidates_ls:
        if itemsets not in rm_duplicate_ls:
            rm_duplicate_ls.append(itemsets)

    print("candidate sets: ", rm_duplicate_ls)"""

    # print("Son pass 2 started:", time.time())

    # SON algorithm pass 2.
    def son_2(sub_baskets):
        counts = collections.defaultdict(int)

        # count frenquency for all candidate itemsets. 
        for basket in sub_baskets:
            for itemset in candidates_ls:
                if set(itemset).issubset(basket) :
                    counts[itemset] += 1

        return [(tuple(sorted(itemset)), count) for itemset, count in counts.items()]
    
    result_RDD = itemsets_RDD.mapPartitions(son_2) \
        .reduceByKey(lambda x, y: x + y) \
        .filter(lambda x: x[1] >= support) \
        .map(lambda x: x[0])
    
    result_ls = result_RDD.collect()

    # print("result")
    # print("Writing output: ", time.time())

    # write results
    # sort by k, sort by alphabatic order, 
    with open(output_file_path, 'w') as f:
        f.write("Candidates:\n")
        son_apriori.write_output_file(f, candidates_ls)

        f.write("Frequent Itemsets:\n")
        son_apriori.write_output_file(f, result_ls)

        
    
    # end time 
    print("Duration: ", time.time()-start)