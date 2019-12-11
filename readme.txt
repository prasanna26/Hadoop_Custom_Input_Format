Instructions to run the program 


The program uses three input arguments.

Arguments 1 and 2 refer to the input and output directory. Argument 3 decides which program will be run.

You can specify one of the following three argumnets:

1)  Argumnet : normal  -> This runs problem 2 of the assignment, which accepts the directory of uncompressed zip files.
2) Argumnet : zip -> This runs problem 3 of the assignment , which accepts the zip file.
3) Argumnet : json -> This runs problem 4 of the assignmnet, which accpets a json file


eg:
If you want to run problem 2, your 3 argumnets would be : cookbook_text, output1.txt, normal. In this case, argumnet[0] is cookbook_text, argument[1] is output_program_2, argument[2]is normal


If you want to run problem 3, your 3 argumnets would be : cookbook_text.zip, output2.txt, normal. In this case, argumnet[0] is cookbook_text.zip, argument[1] is output_program_2, argument[2]is zip


References:
For handling zip files:
http://cutler.io/2012/07/hadoop-processing-zip-files-in-mapreduce/


