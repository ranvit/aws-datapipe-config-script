# aws-datapipe-config-script
Script to generate configuration for Database Migration Pipeline

An example use-case...
![Data pipeline for complex source-dest table pairings](https://github.com/runwithranvit/aws-datapipe-config-script/blob/master/imgs/aws-datapipeline-complex-pairing.png)

What this tool is abstracting for you...
![Table-Pair Abstraction](https://github.com/runwithranvit/aws-datapipe-config-script/blob/master/imgs/aws-datapipeline-table-pair-abstraction.png)

All you have to provide is...
```
{
 ...,
 
 source_databases: [
   {ref1/user/pass/ip},
   {ref2/user/pass/ip},
   ...
 ],

 destination_databases: [
   {ref3/user/pass/ip},
   {ref4/user/pass/ip},
   ...
 ],
 
 table_pairs: [
   {source_table: {name,database_ref1}, dest_table: {name, database_ref3}},
   {source_table: {name,database_ref2}, dest_table: {name, database_ref4}},
   ...
 ],
 
 ...
}
```

Other niceities include
- Activation on a schedule (ONDEMAND by default)
- S3 staging location cleanup upon success
- SNS Alarm upon success
