# topics
Various Clustering implementations in SPARK using DataFrames and in Java.

1. TFIDF - computes TF/IDF vectors. Displays a few rows as conformation, both of TF vectors and TF/IDF.
2. KMeans - performs KMeans clustering. Replicates TF/IDF creation from the TFIDF code.
3. LDA - performs LDA topic clustering and reports topic labels. Replicates TF vector creation from the TFIDF code.
4. LDA - performs LDA topic clustering using NGrams instead of words as tokens. Reports topic labels. 
    Replicates TF/IDF vector creation from the TFIDF code.

Each of these uses a separate static method to create a list of files names which are parallelized into an RDD and then converted to a DataFrame. The files names are parallelized. an RDD.map function loads and tokenizes the text into a POJO in the executors.

Looks for data in a specified directory (-Dsource.path). The data should have one text document per file. 
The file names are used as IDs.  There is some sample data derived from the open-source Enron data set at 

https://drive.google.com/file/d/1tBGYYxmDZ1pSKSYE5I4MvLjqMZOsWbQg/view?usp=sharing

About 5k files spread over 5 subdirectories and all zipped up.



