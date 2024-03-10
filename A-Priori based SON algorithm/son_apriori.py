# all functions needed to perform son algorithm 

from pyspark import SparkContext
import sys, time, math, itertools, collections, random

"""
L_k list of 
 - baskets: list of sets
 - c_k: set of tuples
 - support: integer 
return set of tuples
"""
def l_frequent_sets(baskets, c_k, support):

    frequent_sets = set()
    counts = collections.defaultdict(int)
    
    for b in baskets:
        #print("basket: ", b)
        basket = frozenset(b)
        for itemset in c_k: 
            #print("candidate itemset: ", itemset)
            #print("item tuple:", itemset)
            #print("itemset:", {itemset})
            #print("basketset:", set(b))

            if frozenset(itemset).issubset(basket):
                counts[itemset] += 1

    for itemset, count in counts.items():
        if count >= support:
            frequent_sets.add(itemset)

    
    return frequent_sets


"""
C_k list 
 - l_k_1: set of tuples of itemsets' length = k - 1. 
return set of sets 
"""
def c_candidate_sets(l_k_1, k):

    candidates_set = set()

    # find all candidate itemsets constructed from l_(k-1)
    
    """sorted_l_k_1 = sorted(list(l_k_1), key=lambda x: x)

    for i in sorted_l_k_1:
        first = list(i)
        for j in sorted_l_k_1:
            second = list(j)
            # check for common k-2 prefix 
            if first[:-1] == second[:-1]:
                new_set = tuple(first + [second[-1]])
                if len(new_set) == k:
                    candidates_set.add(new_set)"""

    # find all candidate itemsets constructed from l_(k-1)
    for i in l_k_1:
        for j in l_k_1:
            new_set = tuple(set(i).union(set(j)))
            if (len(new_set) == k):
                candidates_set.add(tuple(sorted(new_set)))

    # remove candidate itemsets whose (k-1) subsets not in l_(k-1)
    candidates_set_filtered = candidates_set.copy()
    for itemset in candidates_set:
        subsets = itertools.combinations(itemset, k-1)
        for subset in subsets:
            if tuple(subset) not in l_k_1:
                candidates_set_filtered.remove(itemset)
                break
    
    return candidates_set_filtered


"""
Change list of sets to list of lists.
"""
def partition(x):
    return (len(x[0]) + random.randint(0, 1000)) % int(n_partitions)



"""
A-priori algorithm to find frequent itemsets for each sub-basket. 
 - baskets: iterator of lists  
 - support: integer
 return a list of sets. 
"""
# A-Priori 
def a_priori(baskets, support):
    # use set to make sure itemsets are unique. 
    all_frequent_itemsets = set()

    # c1, all singletons 
    k = 1
    print("k=", k, ":", time.time())
    # print("c_cur started:", time.time())

    c_cur = set()
    for b in baskets:
        for item in b:
            c_cur.add(tuple([item]))

    # print("candidate sets length: ", len(c_cur), "-", time.time())
    # print("l_cur started:", time.time())

    # l1, frequent singletons
    l_cur = l_frequent_sets(baskets, c_cur, support)
    # print("frequent sets length: ", len(l_cur), "-", time.time())
    
    all_frequent_itemsets.update(l_cur)


    # while exist frequent itemsets for lenth k, continue 
    k = 2
    while(l_cur):
        # print("k=", k, ":", time.time())
        # print("c_cur started:", time.time())
        # candidate sets 
        c_cur = c_candidate_sets(l_cur, k)

        # print("candidate sets length: ", len(c_cur), "-", time.time())
        # print("l_cur started.", time.time())

        # frequent sets
        l_cur = l_frequent_sets(baskets, c_cur, support)

        # print("frequent sets length: ", len(l_cur), "-", time.time())
        

        if len(l_cur) == 0:
            break

        all_frequent_itemsets.update(l_cur)
        
        # update k
        k += 1
    
    return all_frequent_itemsets


"""
write result in the desired format.
 - file: the file to write with 
 - content: list of all itemsets that need to write 
"""
def write_output_file(file, content):
    # sort by length
    content = list(map(lambda x: list(x), content))

    result_dict = dict()
    for itemset in content:
        k = len(itemset)
        if k in result_dict.keys():
            result_dict[k].append(sorted(itemset))
        else:
            result_dict[k] = [sorted(itemset)]
            

    k_ls = sorted(result_dict.keys())
    for i in range(len(k_ls)):
        itemsets_ls = sorted(result_dict[k_ls[i]], key=(lambda x: x))
        line = ""
        for itemset in itemsets_ls:
            line = line + "("
            for item in itemset:
                line = line + f"'{item}', "
            line = line[:-2]
            line = line + "),"
        line = line[:-1]
        
        if i == len(k_ls) - 1:
            file.write(line)
        else:
            file.write(line + "\n\n")



        
        
    


