## CS221 Project - Peterman Search Engine
## Team 5
In this project, we implement:

We implement a disk-based index structure is based on the idea of LSM (Log-Structured Merge tree)
1. write and read
2. merge
3. search(and and or)
4. delete

Chinese and Japanese both Version
use the dictionary called dic_cn and dic_jp under resource directory
have corresponding test case called WordBreakCJKTokenizerTest(6 testcases,3 for Chinese, 3 for Japanese)





### To run this example: 
1. clone this repository using `git clone https://github.com/UCI-Chenli-teaching/spring19-cs221-project.git`
2. run `mvn clean install -DskipTests` in command line
3. open IntelliJ -> Open -> Choose the directory. Wait for IntelliJ to finish importing and building.
4. You can run the `HelloWorld` program under `src/main/java/edu.uci.ics.cs221` package to test if everything works.

