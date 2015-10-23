# BugLocator #



# Introduction #

This page describes BugLocator - the tool we developed for our research work on locating the revelant source code files that need to be changed in order to fix a bug. BugLocator ranks all files based on the textual similarity between the initial bug report and the source code using a new similarity measure, taking into consideration information about similar bugs that have been fixed before.


# Download and Installation #

BugLocator is free for scientific use only. More information about BugLocator can be found at the following paper:
> _Jian Zhou, Hongyu Zhang, and David Lo, Where Should the Bugs be Fixed? More    Accurate Information Retrieval-Based Bug Localization based on Bugs Reports, In  Proc. ICSE 2012, Zurich, Switzerland, June 2012_

This program was developed in Java and is available at:[Download page](http://code.google.com/p/bugcenter/downloads/list).

After unpacking the zip file, you can find the following files:
  * BugLocator.jar
  * ReadMe.txt
  * Data Folder(xml examples)

# How to use #
java -jar BugLocator [-options]

, where options must include:
-b	indicates the bug information file,the bug information file must be an .xml file in the data folder.

-s	indicates the source code directory.

-a	indicates the alpha factor for combining VSMScore and SimiScore.

-o	indicates the result file,the format of result file is "bug id, relevant source code file,rank(start with 0),score".

For example the command for testing on Eclipse SWT system could be:
```
java -jar BugLocator -b E:\data\SWTBugRepository.xml -s E:\swt-3.1\src -a 0.2 -o E:\Data&Tool\output.txt" 
```


If you have any comments/questions regarding the research work or software,
please feel free to [contact us](mailto:hongyu@tsinghua.edu.cn).