from basic_models import Selector, ResultRow


class TrieIterator:
    def __init__(self, collection):
        self.index = 0
        self.collection = collection

        if len(collection) > 0:
            self.key = collection[0]
            self.at_end = False
        else:
            self.key = None
            self.at_end = True

    def open(self):
        pass

    def up(self):
        pass

    def next(self):
        if self.at_end or self.index + 1 >= len(self.collection):
            self.at_end = True
            return

        self.index += 1
        self.key = self.collection[self.index]

    def seek(self, seek_key):
        while not self.at_end and self.key < seek_key:
            self.next()


class LeapFrogJoin:
    iterators: list[TrieIterator]
    at_end: bool
    p: int
    least_key: str | None
    max_key: str | None
    key: str | None

    def __init__(self, iterators: list[TrieIterator]):
        self.iterators = iterators
        self.at_end = False
        self.p = 0
        self.least_key = None
        self.max_key = None
        self.key = None

    def open(self, iterators: list[TrieIterator]):
        pass

    def leapfrog_init(self):
        self.at_end = any(iterator.at_end for iterator in self.iterators)

        if not self.at_end:
            self.iterators = sorted(self.iterators, key=lambda x: x.key)  # Iterators are sorted by key value
            self.p = 0  # Iterator at p has the least key value, Iterator at p-1 has the highest key value (cyclic buffer alike structure)
            self.leapfrog_search()

    def leapfrog_search(self):
        max_key = self.iterators[self.p - 1].key  # if p = 0, then last iterator is fetched

        while True:
            least_key = self.iterators[self.p].key

            if max_key == least_key:
                self.key = least_key
            else:
                self.iterators[self.p].seek(max_key)

                if self.iterators[self.p].at_end:
                    self.at_end = True
                else:
                    max_key = self.iterators[self.p].key
                    self.p = (self.p + 1) % len(self.iterators)

    def leapfrog_next(self):
        self.iterators[self.p].next()

        if self.iterators[self.p].at_end:
            self.at_end = True
        else:
            self.p = (self.p + 1) % len(self.iterators)
            self.leapfrog_search()

    def leapfrog_seek(self, seek_key):
        self.iterators[self.p].seek(seek_key)

        if self.iterators[self.p]:
            self.at_end = True
        else:
            self.p = (self.p + 1) % len(self.iterators)
            self.leapfrog_search()

    # not sure if needed
    def end(self):
        pass

    def sort_iter(self):
        pass

    def iter_size(self):
        pass


class LeapFrogTrieJoin:
    leap_frogs: list[LeapFrogJoin]
    selector: Selector
    depth: int

    def __init__(self, selector: Selector):
        self.selector = selector

    def open(self, selector: Selector):
        self.selector = selector
        self.leap_frogs = []


    def join(self):
        self.depth = -1
        self.triejoin_open()

        yield from self.join_helper()

    def triejoin_open(self):
        if self.depth == len(self.leap_frogs) - 1:
            return False

        self.depth += 1

        leap_frog = self.leap_frogs[self.depth]
        for iterator in leap_frog.iterators:  # open every iterator in leapfrog join at current depth
            iterator.open()

        leap_frog.leapfrog_init()

    def triejoin_up(self):
        leap_frog = self.leap_frogs[self.depth]
        for iterator in leap_frog.iterators:  # open every iterator in leapfrog join at current depth
            iterator.up()

        self.depth -= 1

    def join_helper(self):
        while True:
            #  WE ARE AT THE BOTTOM OF THE JOIN ATTRIBUTES
            if self.depth == len(self.leap_frogs) - 1:  # EXPERIMENTAL! might be this->leapFrogs.size()-1!
                #  TRIE-ITERATORS ARE AT END
                if self.leap_frogs[self.depth].at_end:
                    # RETRACT
                    self.triejoin_up()
                    raise StopIteration

                # TRIE-ITERATORS ARE NOT AT END
                else:
                    #  ADD THIS TUPLE TO JOIN RESULTS!

                    #  NOTE: TODO: Two issues: which partitions do we actually retrieve?
                    #  Just one partition per join relation! (so we need to make sure we only access each corresponding trieIterator once!) (FIXED)
                    #  Second issue: TODO: Why is one partition empty?

                    yield None  # TODO: yield ResultRow

                    #  vector<PARTITION*> partitions;
                    #  //Browse through the trieIterators of each joinRelation exactly once!
                    #  vector<string> relationsBrowsed;
                    #  int parsize = 0;
                    #  long int middle = 1;
                    #  for(int i=0; i<this->leapFrogs.size();i++){
                    #  	for(int j=0; j<(*this->leapFrogs[i]->getIter()).size();j++){
                    #  		//if trieIterator relation name is not in the relationsBrowsed list: add!
                    #  		if(find(relationsBrowsed.begin(), relationsBrowsed.end(), (*this->leapFrogs[i]->getIter())[j]->GetRelationName())==relationsBrowsed.end()){
                    #  			parsize = 0;
                    #  			PARTITION *tmp = (*this->leapFrogs[i]->getIter())[j]->retrieveCurrentPartition(parsize);
                    #  			if(EXT) partitions.push_back(tmp);
                    #  			relationsBrowsed.push_back((*this->leapFrogs[i]->getIter())[j]->GetRelationName());
                    #  			if(!EXT) middle = middle * parsize;
                    #  		}
                    #  	}
                    #  }
                    #
                    #  //Now we have all partitions together, join the tuples!
                    #  PARTITION *results;
                    #  if(EXT) results = joinManyPartitions(partitions);
                    #
                    #  if(!EXT) this->NrJoinResults += middle;
                    #
                    #  if(EXT){
                    #  	for(int j=0;j<results->rows.size();j++){
                    #  	this->NrJoinResults++;
                    #  	if(DEBUG) cout<<"ADDING TUPLE TO JOIN RESULT: (";
                    #  	//Output current tuple for debug reasons
                    #  	if(DEBUG) {for(int i=0; i<results->rows[j].size();i++){
                    #  		cout<<results->rows[j][i]<<",";
                    #  	}
                    #  	cout<<")"<<endl;}
                    #  	}
                    #  }
                    #  if(OUTPUT) addToJoinOutputFile(&results->rows);

            # WE ARE NOT AT THE 0:4,1:0,BOTTOM OF THE JOIN ATTRIBUTES
            else:
                #  TRIE-ITERATORS ARE AT END
                if self.leap_frogs[self.depth].at_end:  # TODO: Query2 crashes at this line! (????)
                    #  RETRACT
                    self.triejoin_up()
                    raise StopIteration
                #  TRIE-ITERATORS ARE NOT AT END
                else:
                    #  OPEN NEXT LEVEL
                    self.triejoin_open()
                    #  LAUNCH NEW JOINHELPER
                    yield from self.join_helper()

            #  SEARCH FOR NEXT TUPLE
            self.leap_frogs[self.depth].leapfrog_next()
