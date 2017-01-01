/*
 * algo_toplayer.cpp
 *
 *  Created on: May 21, 2015
 *      Author: Qi
 */

#include "algo_toplayer.h"
#include "globals.h"
#include "profiling.h"
#include "utils.h"
#include "qp.h"
#include <cstring>
#include <string>
#include <fstream>
#include <algorithm>
#include <string>
#include <bitset>

 using namespace std;

 bool myfunc (const sinfo& a, const sinfo& b){
 	return (a.score > b.score);
 }

 bool sortcdt (const fullinfo& a, const fullinfo& b){
 	return (a.score > b.score);
 }

 bool sort_by_did (const fullinfo& a, const fullinfo& b){
	return (a.did < b.did);
 }	

 bool sort_by_coverage(const fullinfo& a, const fullinfo& b){
 	return (a.coverage > b.coverage);
 }

 bool cmp_by_value(const PAIR& lhs, const PAIR& rhs) {  
 	return lhs.second < rhs.second;  
 }  

 bool compareScore(const entry& i, const entry& j) { return i.score > j.score; }

 bool sort_float (float i, float j) { return (i > j); }

 bool sort_Vec (const sortinfo& a, const sortinfo& b) { return (a.did < b.did); }

// Given an input value, find in which col of the vec belongs to and return
// the index.
// Note that this method must be only used for listlen/intersection vectors.
uint findIdxInBucket(const vector<uint> vec, const uint input) {
  uint idx = 0;

  while (input > vec[idx]) {  // loop until out of range
    ++idx;
  }

  return idx;
}

algo_toplayer::algo_toplayer(unsigned int* pgs){

	pages = pgs;
	
	/*To determine the hashtable size*/
	num_of_did_tracker = 0;

	totaldids_in_layers = 0;

	totaldids_in_pairs = 0;

	num_of_lookups = 0;
	num_of_actual_lookups = 0;

	/*we estimate the final number of results won't be larger than 15000*/
	// Accumulator_Vector.reserve(15000);

	/*to devide the pairs*/
	dem = "+";

	ht = initHash(CONSTS::Query_Budget, 0);

}

uint algo_toplayer::operator() (const vector<vector<float> > &top_layer_model,
								const vector<vector<float> > &term_pair_model,
								const vector<uint> &posting_block_boundry,
								const vector<uint> &posting_block_sizes,
								const vector<uint> &listlen_block_boundry,
								const vector<uint> &intersection_block_boundry,
								CluewebReader* Reader, int qn, toplayers& tls,toplayers& otls, pairlists& pls, pairlists& opls, lptrArray& lps, const int topK, profilerC& p, int qid, uint& budgetsingle, uint& budgetpair, uint& availablebudgetsingle, uint& availablebudgetpair, uint& radixsortsize, uint& ac1, uint& ac2, uint& ac3) {

	// hashTable *ht;
	// fullinfo node;
	// ht = initHash(2091, 0);
	// //int insertHash(hashTable *ht, int key, int elem, int overwrite, vector<T> & a)
	// insertHash(ht, 1261, 0, 0, Accumulator_Vector);
	// node.did = 1261;
	// Accumulator_Vector.push_back(node);
	// insertHash(ht, 3261, 1, 0, Accumulator_Vector);
	// node.did = 3261;
	// Accumulator_Vector.push_back(node);
	// insertHash(ht, 6261, 2, 0, Accumulator_Vector);
	// node.did = 6261;
	// Accumulator_Vector.push_back(node);
	// //int lookupHash(hashTable *ht, int key, vector<T>& a)
	// lookupHash(ht, 3261, Accumulator_Vector);
	// lookupHash(ht, 8261, Accumulator_Vector);
	// exit(0);

	/*number of single terms we have*/
    number_of_singles = tls.cutoffs.size();
    /*number of pairs we have*/
    number_of_pairs = pls.cutoffs.size();
	/*total structures we have*/
    total_number_of_structures = number_of_singles + number_of_pairs;

    get_lens_for_selectionlookups(lps);
	// for(uint i=0; i<lps.size();++i)
	// 	cout<<lps[i]->term<<" "<<lps[i]->unpadded_list_length<<" "<<lensVector[i]<<endl;
	// cout<<lensVector[lensVector.size()-1]<<endl;
	// exit(0);

 	// load_singlelists_tomap(tls, otls);

 	// cout<<"after loading layers"<<endl;

	// decide_termbits();

	// load_pairlists_tomap(pls, opls);

	// cout<<"after loading pairs"<<endl;

	// for(int i=0; i<top_layer_model.size(); ++i){
	// 	for(int j=0; j<top_layer_model[i].size(); ++j){
	// 		cout<<i<<" "<<j<<" "<<top_layer_model[i][j]<<endl;
	// 	}
	// }

	decide_termbits_new(tls, otls, pls, opls);
	Initialize_LookupPruningTable();

	cutoffs.resize(depths.size(), 0);
	cutoffs_pairs.resize(depths_pairs.size(), 0);

	//Only for TopLayer
	p.start(CONSTS::ESSENTIAL);
	/*
	onlineGreedyDepthSelectionAlgorithm(CONSTS::Query_Budget_TopLayer,
                                        top_layer_model,
                                        listlen_block_boundry,
                                        posting_block_boundry,
                                        posting_block_sizes,
                                        depths,
                                        cutoffs);

	// cout<<"after online compute for layers"<<endl;
	
	onlineGreedyDepthSelectionAlgorithm(CONSTS::Query_Budget_TermPair,
                                     term_pair_model,
                                     intersection_block_boundry,
                                     posting_block_boundry,
                                     posting_block_sizes,
                                     depths_pairs,
                                     cutoffs_pairs);
	*/
	for(uint i=0; i<cutoffs.size(); ++i){
		availablebudgetsingle += depths[i];
	}

	for(uint i=0; i<cutoffs_pairs.size(); ++i){
		availablebudgetpair += depths_pairs[i];
	}
	

	onlineGreedyDepthSelectionAlgorithmUnify(CONSTS::Query_Budget, top_layer_model,listlen_block_boundry,
						posting_block_boundry,posting_block_sizes,depths,top_layer_listlengths,term_pair_model,
						intersection_block_boundry,depths_pairs,pairs_listlengths,cutoffs, cutoffs_pairs);

	p.end(CONSTS::ESSENTIAL);


	// uint budgetsingle = 0;
	// uint budgetpair = 0;


	for(uint i=0; i<cutoffs.size(); ++i){
		budgetsingle += cutoffs[i];
	}

	for(uint i=0; i<cutoffs_pairs.size(); ++i){
		budgetpair += cutoffs_pairs[i];
	}

	// cout<<"budget for single: "<<budgetsingle<<endl;
	// cout<<"budget for pair: "<<budgetpair<<endl;

	load_singlelists_new(tls, otls);
  	load_pairlists_new(pls, opls);

  	// for(uint i = 0; i < CONSTS::maxTerm; ++i){
	  // 	for(uint j = 0; j < CONSTS::maxTerm; ++j){
	  // 		cout << lookupPruningTable[i][j] << " ";
	  // 	}
	  // 	cout << endl;
  	// } 

  	p.start(CONSTS::ALLQS);

  	radixsortsize = VecforSort.size();

  	p.start(CONSTS::SORT);
  	// sort(VecforSort.begin(), VecforSort.end(), sort_Vec);	
  	radix_sort();
  	p.end(CONSTS::SORT);

 //  	for(uint i = 0; i < VecforSort.size(); ++i){
	// 	uint tmp = VecforSort[i].did>>4;
	// 	uint bits = VecforSort[i].did&15;
	// 	cout<< tmp <<" "<<bits<<" "<<VecforSort[i].score<<endl;
	// }

	// cout<<"after online compute for pairs"<<endl;
	// adjust_cutoffs();
	// cout<<"after adjust layer cutoffs"<<endl;
	// adjust_cutoffs_pairs();
	// cout<<"after adjust pair cutoffs"<<endl;

	//query processing in here
	p.start(CONSTS::SKIPS);
	// TAAT_merge();
	Vec_Merge();
	p.end(CONSTS::SKIPS);
	
	//test
	// cout<< Accumulator_Vector.size() << endl;
	// for(int i=0; i<Accumulator_Vector.size(); ++i){
	// 	bitset<8> x(Accumulator_Vector[i].kbits);
	// 	cout<<"did: "<<Accumulator_Vector[i].did<<" kbits: "<<x<<" score: "<<Accumulator_Vector[i].score<<endl;
	// 	// out_stream<<"did: "<<Accumulator_Vector[i].did<<" kbits: "<<x<<" score: "<<Accumulator_Vector[i].score<<endl;
	// }
	// exit(0);
	//test

    /*sort the Accumulator by score first before we do lookups, according to score*/
    // sort(Accumulator_Vector.begin(), Accumulator_Vector.end(), sortcdt);

    /*Do lookups on this small set*/
    // if(Accumulator_Vector.size()>CONSTS::Num_Doc_for_Lookups)
    //       Accumulator_Vector.resize(CONSTS::Num_Doc_for_Lookups);
    ac1 = Accumulator_Vector.size();
    p.start(CONSTS::STEP1);
	keeptop(lensVector[0]);
	p.end(CONSTS::STEP1);
	ac2 = Accumulator_Vector.size();
	// p.start(CONSTS::SORT);
    // sort(Accumulator_Vector.begin(), Accumulator_Vector.end(), sort_by_did);
	// p.end(CONSTS::SORT);

	
	p.start(CONSTS::SKIPSB);
	// Do_Lookups(lps);
	Do_SelectionLookups(lps);
	p.end(CONSTS::SKIPSB);

	p.start(CONSTS::STEP2);
	keeptop2(2000);
	p.end(CONSTS::STEP2);

	ac3 = Accumulator_Vector.size();

	/*sort the Accumulator before output*/
	p.start(CONSTS::STEP3);
    sort(Accumulator_Vector.begin(), Accumulator_Vector.end(), sortcdt);
	p.end(CONSTS::STEP3);

    p.end(CONSTS::ALLQS);

	// for(int i=0; i<Accumulator_Vector.size(); ++i){
	// 	bitset<8> x(Accumulator_Vector[i].kbits);
	// 	cout<<"did: "<<Accumulator_Vector[i].did<<" kbits: "<<x<<" score: "<<Accumulator_Vector[i].score<<endl;
	// 	// out_stream<<"did: "<<Accumulator_Vector[i].did<<" kbits: "<<x<<" score: "<<Accumulator_Vector[i].score<<endl;
	// }
	// cout<< Accumulator_Vector.size() << endl;

	writeout_results(qid);
	// // cout<<"after writeout"<<endl;
	cout<< "num of lookups: " << num_of_lookups <<" num of actual lookups: " <<num_of_actual_lookups<<endl;
	return num_of_lookups;
}

void algo_toplayer::Initialize_LookupPruningTable(){
  //initialize the lookup pruning table
  for(uint i = 0; i < CONSTS::maxTerm; ++i){
  	for(uint j = 0; j < CONSTS::maxTerm; ++j)
  		lookupPruningTable[i][j] = 10000;
  }
}

void algo_toplayer::Vec_Merge(){

  //Add a dummy elem in the end of VecforSort, with MaxD
  uint tmp = CONSTS::MAXD << 4;
  sortinfo t_p;
  t_p.did = tmp;
  t_p.score = 0;
  VecforSort.push_back(t_p);

  short mask = 0b11111111;
  float score = 0;

  for(uint i = 0; i < VecforSort.size()-1; ++i){
    //this did contains bit info, if they are not equal, it means they are not the same did and not from the same term.
    if( VecforSort[i+1].did != VecforSort[i].did ){
      uint term_index = VecforSort[i].did&15;
      mask &= ~ (1<<term_index);
      score +=  VecforSort[i].score;

      //pruning the lookups
	  for (uint j = 0; j < number_of_singles; ++j)
        if ( VecforSort[i].score >= lookupPruningTable[term_index][j] )
          mask &= ~ (1<<j);

      if( (VecforSort[i+1].did>>4) > (VecforSort[i].did>>4) ){
        //push the previous elem into accumulator
        fullinfo node;
        node.did = VecforSort[i].did>>4;
        node.score = score;
        node.kbits = mask;
        Accumulator_Vector.push_back(node);

        //reset mask bit and score
        mask = 0b11111111;
        score = 0;
      }
    }
  }
}


void algo_toplayer::decide_termbits_new(toplayers& tls, toplayers& otls, pairlists& pls, pairlists& opls){

	for (int i=0; i<tls.terms.size(); ++i){
		const string term = tls.terms[i];
 		const int length = tls.cutoffs[i];
		const int original_length = otls.cutoffs[i];
 		// const int length = 1000;
		depths.push_back(length);
		top_layer_listlengths.push_back(original_length);
 		TopLayer_Order_Map[term] = i;

 		//cout<<term<<" "<<length<<endl;

		term_orders.push_back(make_pair(term, length)); //for termbits_Map, pair value is the listlength, we decide the order by listlen

	}

	for(int k = 0; k < pls.cutoffs.size(); k++){
		const string pair = pls.pairnames[k];
		const int length = pls.cutoffs[k];
		const int original_length = opls.cutoffs[k];
		depths_pairs.push_back(length);
		pairs_listlengths.push_back(original_length);
		TermPair_Order_Map[pair] = k;
 	}


    /*For a 4 term query, the filter is 00001111*/
 	filter_global = (1<<number_of_singles) - 1;
 	// filter = ~filter;
	bitset<8> y(filter_global);
	// cout<<"The filter for the "<<number_of_singles<<" size query is "<<y<<endl;


	int orders = 0;
	/*sort by listlen, note it will affect lookups orders, modify with caution*/
	sort(term_orders.begin(), term_orders.end(), cmp_by_value);

	for(int i=0; i<term_orders.size(); i++){
		// cout<<term_orders[i].first<<": "<<term_orders[i].second<<endl;
		/*Kbits has 8 bits, 0 means a valid bit, for example, if order = 0, which means the term ranks the first, the corresponding kbit is 1111,1110*/
		termbits_Map[term_orders[i].first] = ~ (1<<orders++); 
	}

	// for(map<string, int>::iterator it = termbits_Map.begin(); it!=termbits_Map.end(); ++it){
	// 	bitset<8> x(it->second);
	// 	cout<<it->first<<": "<<x<<endl;

	// }
}


void algo_toplayer::load_singlelists_new(toplayers& tls, toplayers& otls){
	 // const int buffer_size = 1024*100;
    for (uint i=0; i<tls.terms.size(); ++i){
 		const int length = cutoffs[i];	
 		if(length == 0)
			continue;	
		/*load the did*/
		string did_dir = CONSTS::top_layer_index_did.c_str() + tls.terms[i];
		FILE * loaddids = fopen(did_dir.c_str(), "r");
		if(loaddids==NULL){
			cout<<"can't find file for did"<<endl;
		}
		vector<uint> t_a (length, 0);
		fread(&t_a[0], sizeof(uint), length, loaddids);
		//cout<<length<<" integer are read from: "<<did_dir<<endl;
		fclose(loaddids);

		/*load the score*/
		string score_dir = CONSTS::top_layer_index_score.c_str() + tls.terms[i];
		FILE * loadscores = fopen(score_dir.c_str(), "r");
		if(loadscores==NULL){
			cout<<"can't find file for score"<<endl;
		}
		vector<float> t_f (length, 0);
		fread(&t_f[0], sizeof(float), length, loadscores);
		fclose(loadscores);

		/*Put the info of this single into VecforSort*/
		// for(int j = 0; j < length; j++){
		// 	sortinfo t_p;
		// 	t_p.did  = t_a[j];
		// 	t_p.score = t_f[j];
		// 	VecforSort.push_back(t_p);
		// }

		// /*Put the info of this single into VecforSort, with the shift done*/
		// // for(uint j = 0; j < length; ++j){
		// for(uint j = 0; j < 2; ++j){
		// 	sortinfo t_p;
		// 	bitset<32> x(t_a[j]);
		// 	cout<< t_a[j] << " " <<x<< " " << t_f[j] << endl;
		// 	uint tmp1 = t_a[j] << 4;
		// 	bitset<32> y(tmp1);
		// 	cout<< tmp1 << " " <<y<< " " << t_f[j] << endl;
		// 	uint tmp2 = i;
		// 	bitset<32> z(tmp2);
		// 	cout<< z <<endl;
		// 	uint tmp3 = tmp1|tmp2;
		// 	bitset<32> a(tmp3);
		// 	cout<<a<< " " << t_f[j] << endl;

		// 	t_p.did  = t_a[j];
		// 	t_p.score = t_f[j];
		// 	VecforSort.push_back(t_p);
		// 	uint in;
		// 	cin >> in;
		// }

		cout<<"term "<<i<<" at length "<<length<<" has partial BM25 score of "<<t_f[length-1]<<endl;

		/*Put the info of this single into VecforSort, with the shift done*/
		for(uint j = 0; j < length; ++j){
			sortinfo t_p;
			// bitset<32> x(t_a[j]);
			// cout<< t_a[j] << " " <<x<< " " << t_f[j] << endl;
			uint tmp1 = t_a[j] << 4;
			// bitset<32> y(tmp1);
			// cout<< tmp1 << " " <<y<< " " << t_f[j] << endl;
			// bitset<32> z(i);
			// cout<< z <<endl;
			uint tmp3 = tmp1|i;
			// bitset<32> a(tmp3);
			// cout<<a<< " " << t_f[j] << endl;
			t_p.did  = tmp3;
			t_p.score = t_f[j];
			VecforSort.push_back(t_p);
		}

	}
 	/*for greedy algo*/
 	// cutoffs.resize(depths.size(), 0);
}


void algo_toplayer::load_pairlists_new(pairlists& pls, pairlists& opls){

	for(int k = 0; k < pls.cutoffs.size(); k++){

		const string pair = pls.pairnames[k];
		const int length = cutoffs_pairs[k];
		if(length == 0)
			continue;

		/*Load the dids */
		ifstream index_did;
		string did_dir = CONSTS::intersection_index_did.c_str() + pair;
		index_did.open(did_dir.c_str(), ios::binary);
		unsigned int* t_a = new unsigned int [length];
		index_did.read((char*)t_a, sizeof(unsigned int)*length);
		//cout<<length<<" integer are read from: "<<did_dir<<endl;
		index_did.close();

		/*load the first score*/
		ifstream index_score_first;
		string score_dir_first = CONSTS::intersection_index_score_first.c_str() + pair;
		index_score_first.open(score_dir_first.c_str(), ios::binary);
		float* t_f_1 = new float [length];
		index_score_first.read((char*)t_f_1, sizeof(float)*length);
		// cout<<length<<" float are read from: "<<score_dir_first<<endl;
		index_score_first.close();

		/*load the second score*/
		ifstream index_score_second;
		string score_dir_second = CONSTS::intersection_index_score_second.c_str() + pair;
		index_score_second.open(score_dir_second.c_str(), ios::binary);
		float* t_f_2 = new float [length];
		index_score_second.read((char*)t_f_2, sizeof(float)*length);
		// cout<<length<<" float are read from: "<<score_dir_second<<endl;
		index_score_second.close();


		/*Split the pair here*/
		position = pair.find(dem);
 		term1 = pair.substr(0, position);
 		term2 = pair.substr(position+1, pair.size());
 		// cout << term1 << " " << TopLayer_Order_Map[term1] << endl;
 		// cout << term2 << " " << TopLayer_Order_Map[term2] << endl;
 		// exit(0);

 		cout<<"pair "<<k<<" at length "<<length<<" has partial BM25 score of "<<t_f_1[length-1]<<" + "<<t_f_2[length-1]<<" = "<<t_f_1[length-1] + t_f_2[length-1]<<endl;

		/*Put all the info for this pair into a vector*/
		for(int j = 0; j < length; j++){
			/*put in term1*/
			sortinfo t_p1;
			uint tmp1 = t_a[j] << 4;
			// uint tmp3 = tmp1|i;
			uint tmp3 = tmp1|TopLayer_Order_Map[term1];
			t_p1.did  = tmp3;
			t_p1.score = t_f_1[j];
			VecforSort.push_back(t_p1);

			/*put in term2*/
			sortinfo t_p2;
			// uint tmp3 = tmp1|i;
			tmp3 = tmp1|TopLayer_Order_Map[term2];
			t_p2.did  = tmp3;
			t_p2.score = t_f_2[j];
			VecforSort.push_back(t_p2);
		}

		// /*Put all the info for this pair into a vector*/
		// for(int j = 0; j < length; j++){
		// 	pinfo t_p;
		// 	t_p.did  = t_a[j];
		// 	t_p.s1 = t_f_1[j];
		// 	t_p.s2 = t_f_2[j];
		// 	p_list.push_back(t_p);
		// }

		// Put the minmum score of the pair of the list in the lookuppruning table
		if(length > 0){
			lookupPruningTable[TopLayer_Order_Map[term1]][TopLayer_Order_Map[term2]] = t_f_1[length-1] + t_f_2[length-1];
			lookupPruningTable[TopLayer_Order_Map[term2]][TopLayer_Order_Map[term1]] = t_f_1[length-1] + t_f_2[length-1];
		}

		delete[] t_a;
		delete[] t_f_1;
		delete[] t_f_2;
	}

}

void algo_toplayer::load_singlelists_tomap(toplayers& tls, toplayers& otls){
	 // const int buffer_size = 1024*100;
        for (int i=0; i<tls.terms.size(); ++i){
 		const string term = tls.terms[i];
 		const int length = tls.cutoffs[i];
		const int original_length = otls.cutoffs[i];
 		// const int length = 1000;
		depths.push_back(length);
		top_layer_listlengths.push_back(original_length);
 		TopLayer_Order_Map[term] = i;

 		//cout<<term<<" "<<length<<endl;

		term_orders.push_back(make_pair(term, length)); //for termbits_Map, pair value is the listlength, we decide the order by listlen
		
		/*Hold the layer infomation*/
		vector<sinfo> t_list;

		/*load the did*/
		string did_dir = CONSTS::top_layer_index_did.c_str() + tls.terms[i];
		FILE * loaddids = fopen(did_dir.c_str(), "r");
		if(loaddids==NULL){
			cout<<"can't find file for did"<<endl;
		}
		vector<uint> t_a (length, 0);
		fread(&t_a[0], sizeof(uint), length, loaddids);
		//cout<<length<<" integer are read from: "<<did_dir<<endl;
		fclose(loaddids);

		/*load the score*/
		string score_dir = CONSTS::top_layer_index_score.c_str() + tls.terms[i];
		FILE * loadscores = fopen(score_dir.c_str(), "r");
		if(loadscores==NULL){
			cout<<"can't find file for score"<<endl;
		}
		vector<float> t_f (length, 0);
		fread(&t_f[0], sizeof(float), length, loadscores);
		fclose(loadscores);


		// for(int j = 0; j < 10; j++){
		// 	cout<<t_a[j]<<": "<<t_f[j]<<" ";
		// }
		// cout<<endl;

		// ofstream index;
		// index.open("/home/qw376/sanity_check/"+term+"-*");
		// for(int j = 0; j < length; j++){
		// 		// index<<t_list.at(j).did<<endl;
		//     index<<t_a[j]<<" "<<t_f[j]<<endl;
		// }
		// index.close();
		// exit(0);

		/*Put all the info for this top layer into a vector*/
		for(int j = 0; j < length; j++){
			sinfo t_p;
			t_p.did  = t_a[j];
			t_p.score = t_f[j];
			t_list.push_back(t_p);
		}

		/*put this top layer vector into the top layer map */
		map<string, vector<sinfo>>::iterator it;
		it = TopLayer_Map.find(term);
		if(it==TopLayer_Map.end()){
			TopLayer_Map[term] = t_list;	
			totaldids_in_layers += length;
		}
 	}

 	/*for greedy algo*/
 	// cutoffs.resize(depths.size(), 0);
}

void algo_toplayer::decide_termbits(){
    /*For a 4 term query, the filter is 00001111*/
 	filter_global = (1<<number_of_singles) - 1;
 	// filter = ~filter;
	bitset<8> y(filter_global);
	// cout<<"The filter for the "<<number_of_singles<<" size query is "<<y<<endl;


	int orders = 0;
	/*sort by listlen, note it will affect lookups orders, modify with caution*/
	sort(term_orders.begin(), term_orders.end(), cmp_by_value);

	for(int i=0; i<term_orders.size(); i++){
		// cout<<term_orders[i].first<<": "<<term_orders[i].second<<endl;
		/*Kbits has 8 bits, 0 means a valid bit, for example, if order = 0, which means the term ranks the first, the corresponding kbit is 1111,1110*/
		termbits_Map[term_orders[i].first] = ~ (1<<orders++); 
	}

	// for(map<string, int>::iterator it = termbits_Map.begin(); it!=termbits_Map.end(); ++it){
	// 	bitset<8> x(it->second);
	// 	cout<<it->first<<": "<<x<<endl;

	// }
}

void algo_toplayer::load_pairlists_tomap(pairlists& pls, pairlists& opls){

	/*For paring a pair*/
	position = 0;



	if(pls.cutoffs.size()>0){
		for(int k = 0; k < pls.cutoffs.size(); k++){

			const string pair = pls.pairnames[k];
 			const int length = pls.cutoffs[k];
			const int original_length = opls.cutoffs[k];
 			depths_pairs.push_back(length);
			pairs_listlengths.push_back(original_length);
 			TermPair_Order_Map[pair] = k;

			/*Hold the layer infomation*/
			vector<pinfo> p_list;

			/*Load the dids */
			ifstream index_did;
			string did_dir = CONSTS::intersection_index_did.c_str() + pair;
			index_did.open(did_dir.c_str(), ios::binary);
			unsigned int* t_a = new unsigned int [length];
			index_did.read((char*)t_a, sizeof(unsigned int)*length);
			//cout<<length<<" integer are read from: "<<did_dir<<endl;
			index_did.close();

			/*load the first score*/
			ifstream index_score_first;
			string score_dir_first = CONSTS::intersection_index_score_first.c_str() + pair;
			index_score_first.open(score_dir_first.c_str(), ios::binary);
			float* t_f_1 = new float [length];
			index_score_first.read((char*)t_f_1, sizeof(float)*length);
			// cout<<length<<" float are read from: "<<score_dir_first<<endl;
			index_score_first.close();

			/*load the second score*/
			ifstream index_score_second;
			string score_dir_second = CONSTS::intersection_index_score_second.c_str() + pair;
			index_score_second.open(score_dir_second.c_str(), ios::binary);
			float* t_f_2 = new float [length];
			index_score_second.read((char*)t_f_2, sizeof(float)*length);
			// cout<<length<<" float are read from: "<<score_dir_second<<endl;
			index_score_second.close();


			/*Put all the info for this pair into a vector*/
			for(int j = 0; j < length; j++){
				pinfo t_p;
				t_p.did  = t_a[j];
				t_p.s1 = t_f_1[j];
				t_p.s2 = t_f_2[j];
				p_list.push_back(t_p);
			}

			/*put this top layer vector into the top layer map */
			map<string, vector<pinfo>>::iterator it;
			it = TermPair_Map.find(pair);
			if(it==TermPair_Map.end()){
				TermPair_Map[pair] = p_list;	
				totaldids_in_pairs += length;
			}

			delete[] t_a;
			delete[] t_f_1;
			delete[] t_f_2;
		}
	}

}

void algo_toplayer::adjust_cutoffs(){

	for(iter=TopLayer_Map.begin(); iter!=TopLayer_Map.end(); ++iter){
		cout<<iter->first<<" "<<cutoffs[TopLayer_Order_Map[iter->first]]<<" "<<iter->second.size()<<endl;
		iter->second.resize(cutoffs[TopLayer_Order_Map[iter->first]]);
	}

}

void algo_toplayer::adjust_cutoffs_pairs(){

	for(iter_pair=TermPair_Map.begin(); iter_pair!=TermPair_Map.end(); ++iter_pair){
		cout<<iter_pair->first<<" "<<cutoffs_pairs[TermPair_Order_Map[iter_pair->first]]<<" "<<iter_pair->second.size()<<endl;
		iter_pair->second.resize(cutoffs_pairs[TermPair_Order_Map[iter_pair->first]]);
	}

}

void algo_toplayer::TAAT_merge(){

	//for the single lists merging
	for(map<string, vector<sinfo>>::iterator it = TopLayer_Map.begin(); it!=TopLayer_Map.end(); ++it){

		for(int i = 0; i<it->second.size(); ++i){
			int did = it->second[i].did;
			//check if we have met this did before
			//if it does not exist, then push it in the accumulator and the map 
			int index = lookupHash(ht, did, Accumulator_Vector);
			// cout<<"index: "<<index<<endl;
			if((index==-1)){
				//put it in the map
				insertHash(ht, did, num_of_did_tracker, 0, Accumulator_Vector);
				++num_of_did_tracker;
				//put it in the accumulator
				fullinfo node;
				node.did = did;
				node.score = it->second[i].score;
				node.kbits = termbits_Map[it->first];
				node.coverage = Get_Coverage_From_Kbits(node.kbits);
				//instead of pushing back, we can resize a large vector, then add stuff to it, when we know the space budget
				Accumulator_Vector.push_back(node);
				
				//bitset<8> x(node.kbits);
				//if(did == 39237070) 
				//cout<<node.did<<": "<<node.score<<" "<<x<<" "<<it->first<<endl;

			}else{ //If it does exist, update the score and known level
				Accumulator_Vector[index].score += it->second[i].score;
				Accumulator_Vector[index].kbits &= (termbits_Map[it->first]);
				Accumulator_Vector[index].coverage = Get_Coverage_From_Kbits(Accumulator_Vector[index].kbits);
				
				//bitset<8> x(Accumulator_Vector[iter->second].kbits);
				//if(did == 39237070)
                //cout<<did<<": "<<it->second[i].score<<" "<<x<<" "<<it->first<<endl;				
			
			}
		}

	}//singles finished
	// cout<<"after singles"<<endl;
	//for the pairs merging
	 for(map<string, vector<pinfo>>::iterator it = TermPair_Map.begin(); it!=TermPair_Map.end(); ++it){

	 	//the first value is term1+term2. get the term1 and term2
	 	position = it->first.find(dem);
	 	term1 = it->first.substr(0, position);
	 	term2 = it->first.substr(position+1, it->first.size());

	 	//iterate the list vector for each pair
	 	for(int i = 0; i<it->second.size(); ++i){
	 		int did = it->second[i].did;
	 		//check if we have met this did before
	 		int index = lookupHash(ht, did, Accumulator_Vector);
	 		//If it does not exist, then push it in the accumulator and the map 
	 		if((index==-1)){
	 			//put it in the map
	 			insertHash(ht, did, num_of_did_tracker, 0, Accumulator_Vector);
	 			++num_of_did_tracker;

	 			//put it in the accumulator
	 			fullinfo node;
	 			node.did = did;
	 			node.score = it->second[i].s1 + it->second[i].s2;
	 			node.kbits = (termbits_Map[term1]) & (termbits_Map[term2]);
	 			node.coverage = Get_Coverage_From_Kbits(node.kbits);
	 			//instead of pushing back, we can resize a large vector, then add stuff to it, when we know the space budget
	 			Accumulator_Vector.push_back(node);
				
				//bitset<8> x(node.kbits);
				//if(did == 39237070)
                //cout<<node.did<<": "<<it->second[i].s1<<" "<<it->second[i].s2<<" "<<x<<" "<<it->first<<endl; 

	 		}else{ //if it does exist, update the score and known level
				
                //if(did == 39237070){
				//bitset<8> y(Accumulator_Vector[iter->second].kbits);
				//bitset<8> a(termbits_Map[term1]);
				//bitset<8> b(termbits_Map[term2]);
				//cout<<y<<" "<<a<<" "<<b<<endl;
				//}
				
	 			Accumulator_Vector[index].score = Accumulator_Vector[index].score + 
	 													it->second[i].s1 * (( Accumulator_Vector[index].kbits & (termbits_Map[term1])) != Accumulator_Vector[index].kbits) +
	 													it->second[i].s2 * (( Accumulator_Vector[index].kbits & (termbits_Map[term2])) != Accumulator_Vector[index].kbits);
	 			Accumulator_Vector[index].kbits = Accumulator_Vector[index].kbits & (termbits_Map[term1]) & (termbits_Map[term2]);
	 			Accumulator_Vector[index].coverage = Get_Coverage_From_Kbits(Accumulator_Vector[index].kbits);
				
				//bitset<8> x(Accumulator_Vector[iter->second].kbits);
				//if(did == 39237070){
                                //cout<<did<<": "<<it->second[i].s1<<" "<<it->second[i].s2<<" "<<x<<" "<<it->first<<endl;
				//cout<<Accumulator_Vector[iter->second].score<<endl;
				//}			
	 		}

	 	}
	 }

	/*Only use when we don't do lookups*/
//	sort(Accumulator_Vector.begin(), Accumulator_Vector.end(), sortcdt);
}

void algo_toplayer::Do_Lookups(lptrArray& lps){

	/*sort the Accumulator by score first before we do lookups, according to score*/
//	sort(Accumulator_Vector.begin(), Accumulator_Vector.end(), sortcdt);
	
	/*Do lookups on this small set*/
//       if(Accumulator_Vector.size()>CONSTS::Num_Doc_for_Lookups)
//              Accumulator_Vector.resize(CONSTS::Num_Doc_for_Lookups);
	

//	sort(Accumulator_Vector.begin(), Accumulator_Vector.end(), sort_by_did);
	/*
	cout<<"before lookup"<<endl;
	ofstream out_stream;
        out_stream.open("/home/qw376/1.28/beforelookups", ofstream::app);
	for(int i=0; i<Accumulator_Vector.size(); ++i){
		bitset<8> x(Accumulator_Vector[i].kbits);
		//cout<<"did: "<<Accumulator_Vector[i].did<<" kbits: "<<x<<" score: "<<Accumulator_Vector[i].score<<endl;
		out_stream<<"did: "<<Accumulator_Vector[i].did<<" kbits: "<<x<<" score: "<<Accumulator_Vector[i].score<<endl;
	}
	out_stream.close();
	*/
	/*sort the Accumulator by score first before we do lookups, according to coverage*/
	// sort(Accumulator_Vector.begin(), Accumulator_Vector.end(), sort_by_coverage);

	/*start doing lookups*/
	for(int i=0; i<Accumulator_Vector.size(); ++i){
		vector<short> indexes_to_lookup = Get_indexes_of_termlists_to_do_lookup(Accumulator_Vector[i].kbits);
		// cout<<"size of indexes_to_lookup: "<<indexes_to_lookup.size()<<endl;
	//	cout<<"did: "<<Accumulator_Vector[i].did<<endl;
		for(int j = 0; j<indexes_to_lookup.size(); ++j){
	//		cout<<lps[indexes_to_lookup[j]]->term<<": ";

			/*Calculate score for this did*/
			int did = lps[indexes_to_lookup[j]]->nextGEQ(Accumulator_Vector[i].did);
            if(did == Accumulator_Vector[i].did){
			  lps[indexes_to_lookup[j]]->did = Accumulator_Vector[i].did;
			  const float frequency = lps[indexes_to_lookup[j]]->getFreq();
			  const float score = lps[indexes_to_lookup[j]]->calcScore(frequency,pages[Accumulator_Vector[i].did]);
	//		  cout<<"an actual lookup "<<frequency<<" "<<pages[Accumulator_Vector[i].did]<<" "<<score<<" ";
			  //Update the info for this did
	//		  cout<<"score before: "<<Accumulator_Vector[i].score<<" ";
			  Accumulator_Vector[i].score += score;
	//		  cout<<"score after: "<<Accumulator_Vector[i].score<<" ";
			}
			Accumulator_Vector[i].kbits &= termbits_Map[lps[indexes_to_lookup[j]]->term];
			// Accumulator_Vector[i].coverage = Get_Coverage_From_Kbits(Accumulator_Vector[i].kbits);

			/*Increase number of lookups*/
			++num_of_lookups;
		}
		//cout<<endl;
	/*
		//Put in the lookup budget
		if(num_of_lookups > CONSTS::lookup_budget){
			// cout<<"out of space budget.. "<<CONSTS::lookup_budget<<endl;
			break;
		}
	*/
	}

	/*Add if do sorting after the lookups*/
	sort(Accumulator_Vector.begin(), Accumulator_Vector.end(), sortcdt);
	/*
	ofstream out_stream1;
        out_stream1.open("/home/qw376/1.28/afterlookups", ofstream::app);
	for(int i=0; i<Accumulator_Vector.size(); ++i){
                bitset<8> x(Accumulator_Vector[i].kbits);
               // cout<<"did: "<<Accumulator_Vector[i].did<<" kbits: "<<x<<" score: "<<Accumulator_Vector[i].score<<endl;
       		out_stream1<<"did: "<<Accumulator_Vector[i].did<<" kbits: "<<x<<" score: "<<Accumulator_Vector[i].score<<endl;
	}
	out_stream1.close();
	*/
}

void algo_toplayer::Do_SelectionLookups(lptrArray& lps){
  /* do the lookups for each list from j=0 to m-1 */
  for (uint j = 0; j < number_of_singles; ++j){
    /* do lookups into list j */
    for (uint i = 0; i < Accumulator_Vector.size(); ++i){
      //lookup the score in list j for element results[i], add to element score
      //I just need to check the jth bits, if it's not 0, then look it up, and no need to modify the bits anymore.
    	short kbits = Accumulator_Vector[i].kbits;
    	kbits = kbits>>j;

    	// bitset<8> x(Accumulator_Vector[i].kbits);
    	// bitset<8> a(termbits_Map[lps[j]->term]);
    	// cout<<"1. "<<lps[j]->term<<" "<<a<<" did: "<<Accumulator_Vector[i].did<<" kbits: "<<x<<" score: "<<Accumulator_Vector[i].score<<endl;

    	if(kbits&1){ // the bit is 1, it needs to be looked up
    		int did = lps[j]->nextGEQ(Accumulator_Vector[i].did);
    		if(did == Accumulator_Vector[i].did){
    		  lps[j]->did = Accumulator_Vector[i].did;
			  const float frequency = lps[j]->getFreq();
			  const float score = lps[j]->calcScore(frequency,pages[Accumulator_Vector[i].did]);
			  Accumulator_Vector[i].score += score;
			  // Accumulator_Vector[i].kbits &= termbits_Map[lps[j]->term];
			  // cout<<"a valid lookup"<<endl;
			  ++num_of_actual_lookups;
    		}
    		++num_of_lookups;
    	}
		// bitset<8> y(Accumulator_Vector[i].kbits);
  //   	cout<<"2. "<<lps[j]->term<<" "<<a<<" did: "<<Accumulator_Vector[i].did<<" kbits: "<<y<<" score: "<<Accumulator_Vector[i].score<<endl;
    	// uint h;
    	// cin >> h;
    }
 //    uint h;
	// cin >> h;
  //   if (lensVector[j+1] < lensVector[j]){
		// keeptop(lensVector[j+1]);
		// // cout << lensVector[j+1] << endl;
  //   }
  }
  // Accumulator_Vector.resize(lensVector[lensVector.size()-1]);
  // sort(Accumulator_Vector.begin(), Accumulator_Vector.end(), sortcdt);
}

void algo_toplayer::writeout_results(int qid){

	if(Accumulator_Vector.size()>=CONSTS::num_of_candidate)
		Accumulator_Vector.resize(CONSTS::num_of_candidate);

	// cout<<"Unique Did #: "<< num_of_did_tracker++<<endl;
	
	int count = 0;
	int lookups = 0;
	// int hit = 0;

	ofstream out_stream;
	out_stream.open(CONSTS::Candidates_Pool.c_str(), ofstream::app);
	out_stream<<qid<<":";

	for(int i=0; i<Accumulator_Vector.size(); i++){

		// int n = ~(Accumulator_Vector.at(i).kbits);
		// count = 0;
		// while (n>0) { 
		// 	count = count + (n&1);
		// 	n=n>>1; //Right shift by 1 
		// }
		// lookups += number_of_singles - count; 
		// bitset<8> x(Accumulator_Vector.at(i).kbits);
		// cout<<Accumulator_Vector[i].did<<", "<<Accumulator_Vector[i].score<<", "<<x<<", "<<Accumulator_Vector[i].coverage<<endl;
		out_stream<<Accumulator_Vector.at(i).did<<" ";
	}
	out_stream<<endl;
	out_stream.close();

//	cout<<"Total lookups needed: "<<num_of_lookups<<endl;


}

short algo_toplayer::Get_Coverage_From_Kbits(short okbits){
	
	short filter = filter_global;
	short kbits = okbits;
	short coverage = 0;
	// int n =  ~(kbits);
	while (filter>0) { 
			if(!(kbits&1))
			coverage = coverage + 1;
			// n=n>>1;
			kbits = kbits>>1;
			filter=filter>>1; //Right shift by 1 
	}
	return coverage;
}

vector<short> algo_toplayer::Get_indexes_of_termlists_to_do_lookup(short okbits){

	short filter = filter_global;
	short kbits = okbits;
	vector<short> indexes_to_lookup;
	short index = 0;
	// int n =  ~(kbits);
	// bitset<8> z(kbits);
	// cout<<"kbitz: "<<z<<endl;
	// bitset<8> x(filter);
	// cout<<"filter: "<<x<<endl;

	while (filter>0) { 
			if(kbits&1){
				// cout<<"index to lookup: "<<index<<endl;
				indexes_to_lookup.push_back(index);
			}
			// n=n>>1;
			kbits = kbits>>1;
			filter=filter>>1; //Right shift by 1 
			index++;
	}
	// cout<<"size of indexes_to_lookup: "<<indexes_to_lookup.size()<<endl;
	return indexes_to_lookup;
}


uint algo_toplayer::keeptop(uint topk){

   uint i, j, n, k;
   n = Accumulator_Vector.size();
   k = topk;
   if(k > n) {
   	k = n;
   	return k;
   }
   float s;       /* the splitter element */
   vector<float> f;    /* temporary array for the scores */

   /* copy only the scores into temp array */ 
   // for (i = 0; i < n; ++i)  f.push_back(Accumulator_Vector[i].score);
   for (i = 0; i < n; i+=20) {
    f.push_back(Accumulator_Vector[i].score);
   }

   /* sort temp array to get k-th largest value and put in s */
   sort(f.begin(), f.end(), sort_float);

   /* kth largest value */
   s = f[k/20-1]; 
   // s = f[k/2- 1]; 

   // for (i = 0; i < n; ++i)  cout<<f[i]<<endl;

   // cout<< "kth largest value: "<<s<<endl;

   // cout<<"size before: "<< n << endl;

   /* now collapse the vector by keeping only elements with score >= s */
   for (i = 0, j = 0; i < n; i++){
     if (Accumulator_Vector[i].score >= s)
     { 
        Accumulator_Vector[j] = Accumulator_Vector[i];  /* take entire i-th entry and move it to j 
                                   NOTE: you could have i=j unless you check */
        j++;
     }
   }
   Accumulator_Vector.resize(j);

   // cout<<"size after: "<<  Accumulator_Vector.size() << endl;

   return j;
}

uint algo_toplayer::keeptop2(uint topk){

   uint i, j, n, k, dt, rep, idx;
   n = Accumulator_Vector.size();
   k = topk;
   if(k > n) {
   	k = n;
   	return k;
   }
   float s;       /* the splitter element */
   vector<float> f;    /* temporary array for the scores */

   /* copy only the scores into temp array */ 
   // for (i = 0; i < n; ++i)  f.push_back(Accumulator_Vector[i].score);
   for (i = 0; i < n; i+=10) {
    f.push_back(Accumulator_Vector[i].score);
   }

   /* sort temp array to get k-th largest value and put in s */
   sort(f.begin(), f.end(), sort_float);

   /* kth largest value */
   // s = f[k-1]; 
   // s = f[(k/10) + 15 - 1]; 
   s = f[70 - 1]; 
   // cout << s << endl;

   // for (i = 0; i < n; ++i)  cout<<f[i]<<endl;

   // cout<< "kth largest value: "<<s<<endl;

   // cout<<"size before: "<< n << endl;
 //   j = 600;
 //   rep = 0;
 //   dt = 1;
 //   idx = 70 - 1;
 //   /* now collapse the vector by keeping only elements with score >= s */
 //   // while( rep <= 10 && (j < 500 || j > 550) ){
 //   while( rep <= 10 && j > 550){
	//    for (i = 0, j = 0; i < n; i++){
	//      if (Accumulator_Vector[i].score >= s)
	//      { 
	//         // Accumulator_Vector[j] = Accumulator_Vector[i];  
	//         /* take entire i-th entry and move it to j, NOTE: you could have i=j unless you check */
	//         j++;
	//      }
	//    }

	//    if(j > topk){
	//    	 dt = 10 * ( j - topk ) / topk + 1;
	//    	 // dt = 1;
	//    	 // cout << topk <<" "<< j <<" "<< dt <<" "<<rep<<endl;
	//    	 idx = idx - dt;
	//    	 s = f[idx]; 
	//    	 // cout << s << endl;
	//    }

	//    // if(j < topk){
	//    // 	 dt = 50 * ( topk - j ) / topk + 1;
	//    // 	 // cout << topk <<" "<< j <<" "<< dt <<" "<<rep<<endl;
	//    // 	 idx = idx + dt;
	//    // 	 s = f[idx];
	//    // 	 // cout << s << endl;
	//    // }
	//    // cout << "ac3 size this round: "<<j<<endl;
	//    // uint a;
	//    // cin >> a;
	//    rep ++ ;
	// }

   for (i = 0, j = 0; i < n; i++){
     if (Accumulator_Vector[i].score >= s)
     { 
        Accumulator_Vector[j] = Accumulator_Vector[i];  /* take entire i-th entry and move it to j 
                                   NOTE: you could have i=j unless you check */
        j++;
     }
   }

   Accumulator_Vector.resize(j);

   // cout<<"size after: "<<  Accumulator_Vector.size() << endl;

   return j;

}

// uint algo_toplayer::keeptop(uint topk){

//    uint i, j, n, k;
//    n = Accumulator_Vector.size();
//    k = topk;
//    if(k > n) {
//    	k = n;
//    	return k;
//    }
//    float s;       /* the splitter element */
//    float s2;
//    float s3;
//    float s4;
//    float s5;
//    float s6;
//    float s7;
//    float s8;
//    float s9;
//    float s10;
//    vector<float> f;    /* temporary array for the scores */
//    vector<float> f2; 
//    vector<float> f3;
//    vector<float> f4;
//    vector<float> f5;
//    vector<float> f6;
//    vector<float> f7;
//    vector<float> f8;
//    vector<float> f9;
//    vector<float> f10;

//    cout<<"Accumulator Size: "<< n <<endl;

//    /* copy only the scores into temp array */ 
//    for (i = 0; i < n; ++i)  {
//    	f.push_back(Accumulator_Vector[i].score);
//    }
//    for (i = 0; i < n; i+=2) {
//     f2.push_back(Accumulator_Vector[i].score);
//    }
//    for (i = 0; i < n; i+=3) {
//     f3.push_back(Accumulator_Vector[i].score);
//    }
//    for (i = 0; i < n; i+=4) {
//     f4.push_back(Accumulator_Vector[i].score);
//    }
//    for (i = 0; i < n; i+=5) {
//     f5.push_back(Accumulator_Vector[i].score);
//    }
//    for (i = 0; i < n; i+=6) {
//     f6.push_back(Accumulator_Vector[i].score);
//    }
//    for (i = 0; i < n; i+=7) {
//     f7.push_back(Accumulator_Vector[i].score);
//    }
//    for (i = 0; i < n; i+=8) {
//     f8.push_back(Accumulator_Vector[i].score);
//    }
//    for (i = 0; i < n; i+=9) {
//     f9.push_back(Accumulator_Vector[i].score);
//    }
//    for (i = 0; i < n; i+=10) {
//     f10.push_back(Accumulator_Vector[i].score);
//    }

//    /* sort temp array to get k-th largest value and put in s */
//    sort(f.begin(), f.end(), sort_float);
//    sort(f2.begin(), f2.end(), sort_float);
//    sort(f3.begin(), f3.end(), sort_float);
//    sort(f4.begin(), f4.end(), sort_float);
//    sort(f5.begin(), f5.end(), sort_float);
//    sort(f6.begin(), f6.end(), sort_float);
//    sort(f7.begin(), f7.end(), sort_float);
//    sort(f8.begin(), f8.end(), sort_float);
//    sort(f9.begin(), f9.end(), sort_float);
//    sort(f10.begin(), f10.end(), sort_float);

//    cout<<"size of f: "<<f.size()<<endl;
//    cout<<"size of f2: "<<f2.size()<<endl;
//    cout<<"size of f3: "<<f3.size()<<endl;
//    cout<<"size of f4: "<<f4.size()<<endl;
//    cout<<"size of f5: "<<f5.size()<<endl;
//    cout<<"size of f6: "<<f6.size()<<endl;
//    cout<<"size of f7: "<<f7.size()<<endl;
//    cout<<"size of f8: "<<f8.size()<<endl;
//    cout<<"size of f9: "<<f9.size()<<endl;
//    cout<<"size of f10: "<<f10.size()<<endl;

//    cout<<"index: "<<k-1<<endl;
//    cout<<"index2: "<<k/2-1<<endl;
//    cout<<"index3: "<<k/3-1<<endl;
//    cout<<"index4: "<<k/4-1<<endl;
//    cout<<"index5: "<<k/5-1<<endl;
//    cout<<"index6: "<<k/6-1<<endl;
//    cout<<"index7: "<<k/7-1<<endl;
//    cout<<"index8: "<<k/8-1<<endl;
//    cout<<"index9: "<<k/9-1<<endl;
//    cout<<"index10: "<<k/10-1<<endl;


//    /* kth largest value */
//    s = f[k-1]; 
//    s2 = f2[k/2-1];
//    s3 = f3[k/3-1];
//    s4 = f4[k/4-1];
//    s5 = f5[k/5-1];
//    s6 = f6[k/6-1];
//    s7 = f7[k/7-1];
//    s8 = f8[k/8-1];
//    s9 = f9[k/9-1];
//    s10 = f10[k/10-1];

//    cout<<"highest score: "<<f[0]<<endl;
//    cout<<"lowest score: "<<f[n-1]<<endl;
//    cout<<"s: "<<s<<endl;
//    cout<<"s2: "<<s2<<endl;
//    cout<<"s3: "<<s3<<endl;
//    cout<<"s4: "<<s4<<endl;
//    cout<<"s5: "<<s5<<endl;
//    cout<<"s6: "<<s6<<endl;
//    cout<<"s7: "<<s7<<endl;
//    cout<<"s8: "<<s8<<endl;
//    cout<<"s9: "<<s9<<endl;
//    cout<<"s10: "<<s10<<endl;

//    // exit(0);

//    // for (i = 0; i < n; ++i)  cout<<f[i]<<endl;

//    // cout<< "kth largest value: "<<s<<endl;

//    // cout<<"size before: "<< n << endl;

//    /* now collapse the vector by keeping only elements with score >= s */
//    for (i = 0, j = 0; i < n; i++){
//      if (Accumulator_Vector[i].score >= s)
//      { 
//         Accumulator_Vector[j] = Accumulator_Vector[i];  /* take entire i-th entry and move it to j 
//                                    NOTE: you could have i=j unless you check */
//         j++;
//      }
//    }
//    Accumulator_Vector.resize(j);

//    // cout<<"size after: "<<  Accumulator_Vector.size() << endl;

//    return j;
// }

void algo_toplayer::get_lens_for_selectionlookups(lptrArray& lps){
	for(uint i=0; i<lps.size(); ++i){
		uint tmp;
		for(uint j=0; class_boundries[j]<lps[i]->unpadded_list_length;++j){
			tmp = class_nums[j];
			// cout<<j<<" "<<class_nums[j+1]<<endl;
		}
		lensVector.push_back(tmp);
	}
	lensVector.push_back(500);
}

void algo_toplayer::radix_sort(){

	int i,j;
	uint c1[256], c2[256], c3[256], c4[256];
	uint n = VecforSort.size();
	vector<sortinfo> VecforSort2 = VecforSort;
	/* initialize count arrays */
	for (i = 255; i >= 0; i--)
	{
		c1[i] = 0;
		c2[i] = 0;
		c3[i] = 0;
		c4[i] = 0;
	}
	/* do the counting of frequencies */
	for (i = n-1; i >= 0; i--)
	{
	    c1[VecforSort[i].did & 255]++;
	    c2[(VecforSort[i].did>>8) & 255]++;
	    c3[(VecforSort[i].did>>16) & 255]++;
	    c4[VecforSort[i].did>>24]++;
	}
	/* prefix sum count arrays */
	for (i = 1; i < 256; i++)
	{
	    c1[i] += c1[i-1];
	    c2[i] += c2[i-1];
	    c3[i] += c3[i-1];
	    c4[i] += c4[i-1];
	}

	/* first pass */
	for (i = n-1; i >= 0; i--)
	{
		VecforSort2[--c1[VecforSort[i].did & 255]].did = VecforSort[i].did;
		VecforSort2[c1[VecforSort[i].did & 255]].score = VecforSort[i].score;
	}

  	/* second pass */
	for (i = n-1; i >= 0; i--)
	{
		VecforSort[--c2[(VecforSort2[i].did>>8) & 255]].did = VecforSort2[i].did;
		VecforSort[c2[(VecforSort2[i].did>>8) & 255]].score = VecforSort2[i].score;
	}

 	/* third pass */
    for (i = n-1; i >= 0; i--)
    {
        VecforSort2[--c3[(VecforSort[i].did>>16) & 255]].did = VecforSort[i].did;
        VecforSort2[c3[(VecforSort[i].did>>16) & 255]].score = VecforSort[i].score;
    }

  	/* fourth pass */
    for (i = n-1; i >= 0; i--)
    {
	    VecforSort[--c4[VecforSort2[i].did>>24]].did = VecforSort2[i].did;
	    VecforSort[c4[VecforSort2[i].did>>24]].score = VecforSort2[i].score;
    }

}

/*From Costas*/

entry::entry(uint in_idx, uint in_space, float in_score) {
  idx = in_idx;
  space = in_space;
  score = in_score;
}

entry::entry() {
  idx = 0;
  space = 0;
  score = 0;
}

void entry::setEntry(uint in_idx, uint in_space, float in_score) {
  idx = in_idx;
  space = in_space;
  score = in_score;
}


