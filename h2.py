#!/usr/bin/python

#Emily Erdman
#H2: MongoDB Queries
#6:00


import pymongo,csv

#opens a connection to the mongoDB server running on tempest
connection = pymongo.Connection('tempest', 27017)
db=connection.db_username
db.authenticate('username','password')
#makes a pointer to the database to be edited
sencont = db.sencont

def makeSenateCollection():
    """
    Function creates a new collection called sencont of all of the campaign 
    contributions to US Senate Campaigns from the 1990 to 2010 cycles. 
    """

    #connects to the database and gets the contribfec collection
    db=connection.examples
    db.authenticate('username','password')
    contribfec = db.contribfec
    #finds all of the senate contributions with cycles between 1990 and 2010
    conts = contribfec.find({'cycle':{'$gte':1990,'$lte':2010},'seat':'federal:senate'})
    #loops through all of the documents found and inserts them one at a time
    for cont in conts:
        sencont.insert(cont)


def contributorStates():
    """
    Function that queries the sencont collection without and with indexes
    and prints the results of the searches to a text file.
    """
    
    f = open('/students/eerdman/qtw/h2/data/contributorStates.txt','w')
    #finds all of the contributions from TX in 1992 without any indices
    tx = sencont.find({'contributor_state':'TX','cycle':1992}).explain()
    #writes the results from the find() in a txt file
    f.write('1. Query found %i results by scanning %i records, taking %i milliseconds using a %s.\n'%(tx['n'],tx['nscanned'],tx['millis'],tx['cursor']))
    #creates an index for the cycle and contributor state
    sencont.ensure_index([('cycle',pymongo.ASCENDING),('contributor_state',pymongo.ASCENDING)])
    #finds all of the contributions from TX in 1992 using the indices
    tx2 = sencont.find({'contributor_state':'TX','cycle':1992}).explain()
    #writes the results from the find() in a txt file
    f.write('2. Query found %i results by scanning %i records, taking %i milliseconds using a %s.\n'%(tx2['n'],tx2['nscanned'],tx2['millis'],tx2['cursor']))

def makeOutofState():
    """
    Function constructs a csv file containing the number and amounts of in-state/out-of-state 
    contributions for each candidate
    """
    
    #creates an index on the names of the candidates
    sencont.ensure_index([('recipient_name',pymongo.ASCENDING),
                          ('cycle',pymongo.ASCENDING)])
    f = csv.writer(open('/students/eerdman/qtw/h2/data/invsout.csv','w'), delimiter = ',')
    #creates the first line of the csv file
    f.writerow(['CandidateName','CandidateState','Party','IsIncumbent','Cycle','Won',
                'NumInstateContributions','SunInstateContribution','NumOutofStateContributions',
                'SumOutofStateContributions'])
    #loops through the candidates
    for cand in sencont.distinct('recipient_name'):
        print cand
        #gets all of the contributions for that candidate
        contribs = sencont.find({'recipient_name':cand})
        #finds all of the distinct cycles in which the candidate campaigned
        cycles = contribs.distinct('cycle')
        #loops through the cycles
        for cycle in cycles:
            #finds a random document to get the relevant candidate data
            ex = sencont.find_one({'recipient_name':cand,'cycle':cycle})
            state = ex['recipient_state']
            party = ex['recipient_party']
            incumb = ex['seat_status']
            won = ex['seat_result']
            #inititalizes the counts for each cycle
            inStateCont = 0
            outStateCont = 0
            amountInState = 0
            amountOutState = 0
            #loops through all of the contributions and adds up the 
            #in state and out of state contributions
            for contrib in sencont.find({'recipient_name':cand,'cycle':cycle}):
                if contrib['contributor_state']==state:
                    inStateCont += 1
                    amountInState += contrib['amount']
                else:
                    outStateCont += 1
                    amountOutState += contrib['amount']
            #writes the data for that cycle to the csv file
            f.writerow([cand,state,party,incumb,cycle,won,inStateCont,
                       amountInState,outStateCont,amountOutState])
                                
    

if __name__ == '__main__':
    #makeSenateCollection()
    #contributorStates()
    makeOutofState()

