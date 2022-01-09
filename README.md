# DSPS_ASSIGNMENT2_HADOOP

## Orri Nehamkin 209831437

I implemented the project using 6 steps:

# Steps 1-3
Step1: M-R over the 1gram corpus, also for each word we will send <*,val> to the reducer to get the total number of words

Step2: M-R over the 2gram corpus

Step3: M-R over the 3gram corpus

# Step 4-6
## Step 4: 
Map:

Input: output from step2 and step3
* Step2 (key: <w1,w2>) (value: count)
* Step3 (key: <w1,w2, w3>) (value: count)

Output: 
* if the <key, value> originated from step2 -> emit (key:<w1,w2>) (value: count)
* if the <key, value> originated from step3 -> (key: <w1,w2, w3>) (value: count) then we will emit twice:
1. emit(key: <w1,w2>, value: <w1w2w3, count>>
2. emit(key: <w2,w3>, value: <w1w2w3, count>>
      
Reduce:
Input: (key: <wi,wj>) <values: <wi,wj,wk, countk> | <wk,wi,wj, countk> | **count**)    
first we will find **count** 
then for every <w1,w2,w3> we will check wether wi,wj are the first two words or the second
* if they are the first two -> emit (key:<wi,wj,wk>, value: "w1w2"+ wi+ wj + **count**>
* if they are the second two -> emit (key:<wk,wi,wj>, value: "w2w3"+ wi+ wj + **count**>
    
## Step5:

Map:

Input: output from Step3 and Step4
* Step3 (key: <w1,w2,w3>) (value: count)
* Step4 (key: <w1,w2, w3>) (value: <"w1w2" w1 w2 count>, <"w2w3" w2 w3 count>)
emit (key, value)

Reduce: 
Initialize map containing the <wi, count> all the values from 1gram corpus. _Scalable only 10 megabytes_

Input: (key: <w1,w2,w3>) <values: <"w1w2",w1,w2,count> | <"w2w3",w2,w3,count> | **count**)    
* Parse the values to N1,N2,N3  C1,C2, K2,K3 and calculate the probability
* emit (key, probability)


## Step 6:

Input: output from step5

(key: <w1,w2,w3>) (value: probability)

emit(key, value)

WritableComparable:
Compare between the keys and the probability so that w1,w2 is lexicographic and w1,w2,wi and w1,w2,wj are compared by their probability

upload to s3


Example from output: 

מתן תורה לא 0.020437378008557452	
מתן תורה היו 0.011574900588627956	
מתן תורה בא 0.011190661190453416	
מתן תורה ״ 0.010705442446214644	
מתן תורה ( 0.009162479092896986	
מתן תורה : 0.008778701236930718	
מתן תורה בסיני 0.008628381603436947	
מתן תורה ) 0.007838043049413356	
מתן תורה הוא 0.007667467099175148	
מתן תורה לישראל 0.006169236816693254	
מתן תורה ולא 0.005173396005951622	
מתן תורה ולא 0.005165774951900869	
מתן תורה בהר 0.0046860835606359185	
מתן תורה לא 0.004649609231247832	
מתן תורה היתה 0.004425983104232784	
מתן תורה עד 0.004231189670658613
...





    
    




