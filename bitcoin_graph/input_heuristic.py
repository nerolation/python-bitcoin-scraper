class InputHeuristic:
    
    def __init__(self):
        self.clusters       = {}
        self.knownNodes     = set()
        self.clusterLookUp  = {}
        self.clusterIndex   = 0 

    def _get_cluster(self, inp):
        if inp in self.knownNodes:
            return self.clusterLookUp[inp]
            
    def _merge_clusters(self, n):
        #print(f"merging {str(n)}")
        #print(clusters)
        for i in n[1:]:
            #print(f"adding {str(clusters[i])} to {str(clusters[n[0]])}")
            for j in self.clusters[i]:
                self.clusterLookUp[j] = n[0]

            self.clusters[n[0]].update(self.clusters[i])
            del self.clusters[i]
        #print(clusters)
        return n[0]


    def _inputs_to_cluster(self, inputlist, c): 
        #print(f"adding {inputlist} to {c}")
        self.knownNodes.update(inputlist)
        #print(c)
        #print(clusters)
        #print(clusters[c])
        self.clusters[c].update(inputlist)
        for i in inputlist:
            self.clusterLookUp[i] = c


    def _create_entry(self, inputlist):
        self.clusters[self.clusterIndex] = set(inputlist)
        #print(inputlist, clusterLookUp)
        for i in inputlist:
            self.clusterLookUp[i] = self.clusterIndex
        self.clusterIndex += 1
        self.knownNodes.update(inputlist)
        return self.clusterIndex - 1

    # Provide array with input addresses
    def handle_inputs(self, inputs):
        #print()
        #print(f"processing {str(i)}")
        n = set()
        for _u in inputs:
            c = self._get_cluster(_u)
            if c != None:
                #print(f"returned {c}")
                n.add(c)
        n = list(n)
        if n:
            if len(n) > 1:
                n.sort()
                c = self._merge_clusters(n)
            else:
                c = n[0]

            self._inputs_to_cluster(inputs, c)
             
        else:
            c = self._create_entry(inputs)
        
        # Return Cluster Index of input array
        return c