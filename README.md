## CS221 Project - Peterman Search Engine

## Project 1
In this project, we implement:
1. Punctuation Tokenizer
2. PorterStemmer
3. WordBreak--English version (Use Dynamic programming)
4. WordBreakCKJ(class)

Chinese and Japanese both Version
use the dictionary called dic_cn and dic_jp under resource directory
have corresponding test case called WordBreakCJKTokenizerTest(6 testcases,3 for Chinese, 3 for Japanese)



## Project 2
In this project, we implement:

Based on previous project(analyer), it tokenlize and stem the input document. We implement a disk-based index structure is based on the idea of LSM (Log-Structured Merge tree). We use the one file to store the words dictionary and the the document ids. Beside, we use multi-thread merging and searching to improve the performance. 

1. write and read
2. merge
3. search(and and or)
4. delete

### To run this example: 
1. run `mvn clean install -DskipTests` in command line
2. open IntelliJ -> Open -> Choose the directory. Wait for IntelliJ to finish importing and building.
3. You can run the `HelloWorld` program under `src/main/java/edu.uci.ics.cs221` package to test if everything works.

