# topics
Various Clustering implementations in SPARK using DataFrames and in Java.

1. TFIDF - computes TF/IDF vectors
2. KMeans - performs KMeans clustering 
3. LDA - performs LDA topic clustering and reports topic labels

Looks for data in a specified directory (-Dsource.path). The data should have one text document per file. 
The file names are used as IDs.  There is some sample data derived from the open-source Enron data set at 

https://drive.google.com/file/d/1tBGYYxmDZ1pSKSYE5I4MvLjqMZOsWbQg/view?usp=sharing

About 5k files spread over 5 subdirectories and all zipped up.


