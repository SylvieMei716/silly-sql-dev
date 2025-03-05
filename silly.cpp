// Project Identifier: C0F4DFE8B340D81183C208F70F9D2D797908754D

// Programmer: Xinyu Mei
// Date: Mar 12, 2024
// This program will emulate a basic relational database with an 
// interface based on a subset of a standard query language

#include <cstddef>
#include <cstdio>
// #include <sstream>
#include <vector>
#include <deque>
#include <queue>
#include <iostream>
#include <algorithm>
#include <getopt.h>
#include <string>
#include "TableEntry.h"
// #include <iomanip>
#include <unordered_map>
#include <map>

struct compareGreater {
    bool operator()(const std::vector<TableEntry> &row);
    size_t idx;
    TableEntry entry;
    compareGreater(size_t idx, TableEntry entry) : idx(idx), entry(entry) {}
    
};

struct compareLess {
    bool operator()(const std::vector<TableEntry> &row);
    size_t idx;
    TableEntry entry;
    compareLess(size_t idx, TableEntry entry) : idx(idx), entry(entry) {}
    
};

struct compareEqual {
    bool operator()(const std::vector<TableEntry> &row);
    size_t idx;
    TableEntry entry;
    compareEqual(size_t idx, TableEntry entry) : idx(idx), entry(entry) {}
    
};

class Table {
    public:
        std::unordered_map<std::string, size_t> colNames;
        std::vector<EntryType> colTypes;
        std::vector<std::vector<TableEntry>> rows;
        std::map<TableEntry, std::vector<size_t>> bstIndex;
        std::unordered_map<TableEntry, std::vector<size_t>> hashIndex;
        size_t colWithIdx;
        std::string typeIdx;
        // Table() : colNames(0), colTypes(0), rows(0), hashIndex(0) {}
        void generate(std::string &tableName);
        void insert(std::string &tableName);
        void print(std::string &tableName, bool &quietMode);
        size_t printBST(bool &quietMode, char &op, const TableEntry& a, std::vector<size_t>& printColIndexVec);
        size_t printHASH(bool &quietMode, const TableEntry& a, std::vector<size_t>& printColIndexVec);
        size_t printWHERE(bool &quietMode, char &op, const TableEntry& a, size_t &idx, std::vector<size_t>& printColIndexVec);
        void deleteRow(std::string &tableName);
        TableEntry type_helper(size_t idx);
};

class Database {
    public:
        void getOptions(int argc, char **argv);
        
        bool quietMode = false;
        std::unordered_map<std::string, Table> tables;
        // Database(): tables(0) {}
        void create(std::string &tableName);
        void remove(std::string &tableName);
        void join(std::string &table1, std::string &table2, bool &quietMode);
        void execute();
};


int main(int argc, char** argv) {
    std::ios_base::sync_with_stdio(false);
    std::cin >> std::boolalpha;
    std::cout << std::boolalpha;
    // std::cin.tie(NULL);
    Database db;
    db.getOptions(argc, argv);
    db.execute();
}

bool compareGreater::operator()(const std::vector<TableEntry> &row) {
    return row[idx] > entry;
}


bool compareLess::operator()(const std::vector<TableEntry> &row) {
    return row[idx] < entry;
}

bool compareEqual::operator()(const std::vector<TableEntry> &row) {
    return row[idx] == entry;
}

void Table::insert(std::string &tableName) {
    std::string trash;
    size_t numNewRows;
    std::cin >> numNewRows >> trash;

    size_t oldSize = rows.size();
    size_t numCols = colTypes.size();
    rows.resize(oldSize + numNewRows);

    for (size_t i = oldSize; i < oldSize + numNewRows; i++) {
        rows[i].reserve(numCols);
        for (size_t j = 0; j < numCols; j++) {
            if (colTypes[j] == EntryType::Bool) {
                bool newEntry;
                std::cin >> newEntry;
                rows[i].emplace_back(newEntry);
            }
            else if (colTypes[j] == EntryType::Int) {
                int newEntry;
                std::cin >> newEntry;
                rows[i].emplace_back(newEntry);
            }
            else if (colTypes[j] == EntryType::String) {
                std::string newEntry;
                std::cin >> newEntry;
                rows[i].emplace_back(newEntry);
            }
            else if (colTypes[j] == EntryType::Double) {
                double newEntry;
                std::cin >> newEntry;
                rows[i].emplace_back(newEntry);
            }
        }
    }
    if (typeIdx == "bst") {
        // bstIndex.clear();
        for(size_t j = oldSize; j < rows.size(); j++) {
            bstIndex[rows[j][colWithIdx]].emplace_back(j);
        }
    }
    else if(typeIdx == "hash") {
        // hashIndex.clear();
        for(size_t j = oldSize; j < rows.size(); j++) {
            hashIndex[rows[j][colWithIdx]].emplace_back(j);
        }
    }

    std::cout << "Added " << numNewRows << " rows to " << tableName 
              << " from position " << oldSize << " to " << oldSize + numNewRows - 1
              << "\n";

}

void Table::print(std::string &tableName, bool &quietMode) {
    //print
    size_t numCols;
    std::cin >> numCols;
    std::vector<std::string> colsToPrint;
    colsToPrint.resize(numCols);

    for (size_t i = 0; i < numCols; i++) {
        std::string colToPrint;
        std::cin >> colToPrint;
        if (colNames.find(colToPrint) == colNames.end()) {
            std::cout << "Error during PRINT: " << colToPrint 
                      << " does not name a column in " << tableName << "\n";
            std::string trash;
            getline(std::cin, trash);
            return;
        }
        colsToPrint[i] = colToPrint;
    }

    std::string printCondition;
    std::cin >> printCondition;
    // if (!quietMode) {
    //     for (size_t i = 0; i < numCols; i++) {
    //         std::cout << colsToPrint[i] << " ";
    //     }
    //     std::cout << "\n";
    // }
    
    if (printCondition == "ALL") {
        // print all
        if (!quietMode) {
            for (size_t i = 0; i < numCols; i++) {
                std::cout << colsToPrint[i] << " ";
            }
            std::cout << "\n";
        }
        if (!quietMode) {
            // print all lines
            // std::cout << "% ";
            
            
            for (size_t i = 0; i < rows.size(); i++) {
                for (size_t j = 0; j < numCols; j++) {
                    auto it = colNames.find(colsToPrint[j]);
                    std::cout << rows[i][it->second] << " ";
                }
                std::cout << "\n";
            }
        }
        std::cout << "Printed " << rows.size() << " matching rows from "
                      << tableName << "\n";
    }
    else if (printCondition == "WHERE") {
        // print where
        std::string colToCompare;
        std::cin >> colToCompare;
        if (colNames.find(colToCompare) == colNames.end()) {
            std::cout << "Error during PRINT: " << colToCompare 
                      << " does not name a column in " << tableName << "\n";
            std::string trash;
            getline(std::cin, trash);
            return;
        }
        if (!quietMode) {
            for (size_t i = 0; i < numCols; i++) {
                std::cout << colsToPrint[i] << " ";
            }
            std::cout << "\n";
        }
        char op;
        std::cin >> op;
        auto it = colNames.find(colToCompare);
        size_t colIdx = it -> second;
        TableEntry entry = type_helper(colIdx);
        std::vector<size_t> printColIndexVec;
        printColIndexVec.resize(numCols);
        for (size_t i = 0; i < numCols; i++) {
            auto it = colNames.find(colsToPrint[i]);
            printColIndexVec[i] = it->second;
        }
        // printColIndexVec.push_back(it -> second);
        // printColNameVec.push_back(colName);
        size_t numRowPrinted = printWHERE(quietMode, op, entry, colIdx, printColIndexVec);

        std::cout << "Printed " << numRowPrinted << " matching rows from "
                  << tableName << "\n";
    }
}

size_t Table::printBST(bool &quietMode, char &op, const TableEntry &a, std::vector<size_t> &printColIndexVec) {
    //
    size_t count = 0;
    if (op == '='){
        auto it = bstIndex.find(a);
        if (it != bstIndex.end()){
            if (!quietMode){
                for (size_t rowIdx:it -> second){
                    for (size_t colIdx:printColIndexVec){
                        std::cout << rows[rowIdx][colIdx] << ' ';
                    }
                    std::cout << '\n';
                    count++;
                }
            }
            else {
                count += it->second.size();
                // std::cout << "size is" << it->second.size();
            }
        }
    }
    else if (op == '<'){
        auto it = bstIndex.lower_bound(a);
        // if (it != bstIndex.begin()){
        for (auto i = bstIndex.begin(); i != it; i++){
            for (size_t rowIdx:i -> second){
                if (!quietMode){
                    for (size_t colIdx:printColIndexVec){
                        std::cout << rows[rowIdx][colIdx] << ' ';
                    }
                    std::cout << '\n';
                }
                count++;
            } 
        }
        // }
    }
    else{
        auto it = bstIndex.upper_bound(a);
        // if (it != bstIndex.end()){
        for (auto i = it; i != bstIndex.end(); i++){
            for (size_t rowIdx:i -> second){
                if (!quietMode){
                    for (size_t colIdx:printColIndexVec){
                        std::cout << rows[rowIdx][colIdx] << ' ';
                    }
                    std::cout << '\n';
                }
                count++;
            } 
        }
        // }
    }
    return count;
}

size_t Table::printHASH(bool &quietMode, const TableEntry &a, std::vector<size_t> &printColIndexVec) {
    size_t count = 0;
    auto it = hashIndex.find(a);
    if (it != hashIndex.end()){
        if (!quietMode){
            for (auto i:it -> second){
                for (auto j:printColIndexVec){
                    std::cout << rows[i][j] << ' ';
                }
                count++;
                std::cout << '\n';
            }
        }
        else {
            count += it->second.size();
            // std::cout << "size is" << it->second.size();
        }
    }
    return count;
}

size_t Table::printWHERE(bool &quietMode, char &op, const TableEntry &a, size_t &idx, std::vector<size_t> &printColIndexVec) {
    if (!bstIndex.empty() && colWithIdx == idx) {
        return printBST(quietMode, op, a, printColIndexVec);
    }
    else if (!hashIndex.empty() && colWithIdx == idx && op == '='){
        return printHASH(quietMode, a, printColIndexVec);
    }
    else {
        size_t count = 0;
        if (op == '>') {
            for (size_t i = 0; i < rows.size(); i++){
                compareGreater c(idx, a);
                if (c(rows[i])){
                    if (!quietMode){
                        for (auto j:printColIndexVec){
                            std::cout << rows[i][j] << ' ';
                        }
                        std::cout << '\n';
                    }
                    count++;
                }
            }
        }
        else if (op == '<') {
            for (size_t i = 0; i < rows.size(); i++){
                compareLess c(idx, a);
                if (c(rows[i])){
                    if (!quietMode){
                        for (auto j:printColIndexVec){
                            std::cout << rows[i][j] << ' ';
                        }
                        std::cout << '\n';
                    }
                    count++;
                }
            }
        }
        else {
            for (size_t i = 0; i < rows.size(); i++){
                compareEqual c(idx, a);
                if (c(rows[i])){
                    if (!quietMode){
                        for (auto j:printColIndexVec){
                            std::cout << rows[i][j] << ' ';
                        }
                        std::cout << '\n';
                    }
                    count++;
                }
            }
        }
        return count;
    }
}

void Table::deleteRow(std::string &tableName) {
    // delete where
    std::string trash;
    std::cin >> trash;
    std::string colToCompare;
    std::cin >> colToCompare;
    char op;
    std::cin >> op;
    // size_t valToCompare;
    // std::cin >> valToCompare;
    size_t oldSize = rows.size();
    if (colNames.find(colToCompare) == colNames.end()) {
        std::cout << "Error during DELETE: " << colToCompare 
                  << " does not name a column in " << tableName << "\n";
        std::string trash;
        getline(std::cin, trash);
        return;
    }
    
    size_t i = colNames.find(colToCompare)->second;

    TableEntry entry = type_helper(i);
    
    if (op == '>') {
        rows.erase(remove_if(rows.begin(), rows.end(), compareGreater(i, entry)), rows.end());
    }
                                
    if (op == '<') {
        rows.erase(remove_if(rows.begin(), rows.end(), compareLess(i, entry)), rows.end());
    }
            
    if (op == '=') {
        rows.erase(remove_if(rows.begin(), rows.end(), compareEqual(i, entry)), rows.end());
    }

    std::cout << "Deleted " << oldSize - rows.size() << " rows from " << tableName << "\n";
            
    if(typeIdx == "bst") {
        bstIndex.clear();
        for(size_t j = 0; j < rows.size(); j++) {
            bstIndex[rows[j][colWithIdx]].emplace_back(j);
        }
    }
    if(typeIdx == "hash") {
        hashIndex.clear();
        for(size_t j = 0; j < rows.size(); j++) {
            hashIndex[rows[j][colWithIdx]].emplace_back(j);
        }
    }
}

TableEntry Table::type_helper(size_t idx) {
    if (colTypes[idx] == EntryType::Int) {
        int valInt;
        std::cin >> valInt;
        return TableEntry{valInt};
    }
    else if (colTypes[idx] == EntryType::Double) {
        double valDouble;
        std::cin >> valDouble;
        return TableEntry{valDouble};
    }
    else if (colTypes[idx] == EntryType::Bool) {
        bool valBool;
        std::cin >> valBool;
        return TableEntry{valBool};
    }
    else {
        std::string valString;
        std::cin >> valString;
        return TableEntry{valString};
    }
}

void Table::generate(std::string &tableName) {
    // generate index
    std::string idxType;
    std::cin >> idxType;
    std::string trash;
    std::cin >> trash;
    std::cin >> trash;
    std::string colToIdx;
    std::cin >> colToIdx;

    if (colNames.find(colToIdx) == colNames.end()) {
            std::cout << "Error during GENERATE: " << colToIdx
                      << " does not name a column in " << tableName << "\n";
            return;
    }

    hashIndex.clear();
    bstIndex.clear();

    auto it = colNames.find(colToIdx);
    size_t colIdx = it -> second;
    colWithIdx = colIdx;
    typeIdx = idxType;

    if (idxType == "hash") {
        // hash idx
        for (size_t i = 0; i < rows.size(); i++) {
            // hashIndex.emplace(rows[i][colIdx], i);
            hashIndex[rows[i][colIdx]].emplace_back(i);
        }
        std::cout << "Created hash index for table " << tableName << " on column "
                  << colToIdx << ", with " << hashIndex.size() << " distinct keys\n";
    }
    else if (idxType == "bst") {
        // bst idx
        for (size_t i = 0; i < rows.size(); i++) {
            // bstIndex.emplace(rows[i][colIdx], i);
            bstIndex[rows[i][colIdx]].emplace_back(i);
        }
        std::cout << "Created bst index for table " << tableName << " on column "
                  << colToIdx << ", with " << bstIndex.size() << " distinct keys\n";
    }

}

void Database::getOptions(int argc, char **argv) {
    int option_index = 0, option = 0;
    opterr = false;

    struct option longOpts[] = {{ "quiet", no_argument, nullptr, 'q'},
                                { "help", no_argument, nullptr, 'h' },
                                { nullptr, 0, nullptr, '\0' }};
    
    while ((option = getopt_long(argc, argv, "qh", 
                                 longOpts, &option_index)) != -1) {
        switch (option) {
            case 'q':
                quietMode = true;
                break;
            
            case 'h':
                std::cout << "This program reads a txt file that contains \n"
                          << "several commands, it then process the database \n"
                          << "and tables following the rules.\n"
                          << "Usage: \'./silly\n\t[--quiet | -q] <N>\n"
                          <<                      "\t[--help | -h]\n"
                          <<                      "\t< <TXT input map file>\n" 
                          <<                      "\t< <TXT output file>\n";    
                exit(0);
        }
    }
}

void Database::create(std::string &tableName) {
    std::string colType;
    std::string colName;
    int numCol;
    tables[tableName];
    // size_t index = 0;

    std::cin >> numCol;
    // newTable.colTypes.resize(numCol);

    // auto it = tables.begin();
    auto it = tables.find(tableName);

    for (int i = 0; i < numCol; i++) {
        std::cin >> colType;
        if (colType == "bool") {
            it->second.colTypes.emplace_back(EntryType::Bool);
        }
        else if (colType == "int") {
            it->second.colTypes.emplace_back(EntryType::Int);
        }
        else if (colType == "string") {
            it->second.colTypes.emplace_back(EntryType::String);
        }
        else if (colType == "double") {
            it->second.colTypes.emplace_back(EntryType::Double);
        }
    }
    std::cout << "New table " << tableName << " with column(s)";
    for (int i = 0; i < numCol; i++) {
        std::cin >> colName;
        it->second.colNames[colName] = i;
        std::cout << " " << colName;
    }
    std::cout << " created\n";

    // tables[tableName] = newTable;
}

void Database::remove(std::string &tableName) {
    tables.erase(tables.find(tableName));
    std::cout << "Table " << tableName << " removed\n";
}

void Database::join(std::string &table1, std::string &table2, bool &quietMode) {
    // join two tables
    std::string trash;
    std::string col1;
    std::string col2;
    size_t num;
    std::vector<std::pair<bool, size_t>> colsToPrint; // tablenum, col
    
    std::cin >> trash >> col1 >> trash >> col2 >> trash >> trash >> num;
    
    Table& t1 = tables[table1];
    Table& t2 = tables[table2];
    
    if(t1.colNames.find(col1) == t1.colNames.end()) {
        std::cout << "Error during JOIN: " << col1 << " does not name a column in " << table1 << '\n';
        getline(std::cin, col1);
        return;
    }
    if(t2.colNames.find(col2) == t2.colNames.end()) {
        std::cout << "Error during JOIN: " << col2 << " does not name a column in " << table2 << '\n';
        getline(std::cin, col2);
        return;
    }
    
    // std::unordered_map<TableEntry, std::vector<size_t>> tempHash1;
    std::unordered_map<TableEntry, std::vector<size_t>> tempHash2;

    colsToPrint.resize(num);
    
    size_t t1_col = t1.colNames.find(col1) -> second;
    size_t t2_col = t2.colNames.find(col2) -> second;
    
    // for(size_t i = 0; i < tables[table1].rows.size(); i++) {
    //     auto hash1Idx = tables[table1].rows[i][t1_col];
    //     tempHash1[hash1Idx].emplace_back(i); // i = index of row for entry
    // }
    if (t2.hashIndex.empty() || t2.colWithIdx != t2_col) {
        for(size_t i = 0; i < t2.rows.size(); i++) {
            auto hash2Idx = t2.rows[i][t2_col];
            tempHash2[hash2Idx].emplace_back(i); // i = index of row for entry
        }
    }
    else {
        tempHash2 = t2.hashIndex;
    }
    
    //print cols
    std::string colPrint;
    int table_num;
    for(size_t i = 0; i < num; i ++) {
        std::cin >> colPrint >> table_num;
        if (!quietMode) {
            std::cout << colPrint << " ";    
        }
    
        if(table_num == 1) {
            colsToPrint[i] = {true, t1.colNames.find(colPrint) -> second};
        }
        if(table_num == 2) {
            colsToPrint[i] = {false, t2.colNames.find(colPrint) -> second};
        }
    }
    if (!quietMode) {
        std::cout << '\n';
    }
    
    size_t count = 0;
// iterator to tables[table1, table2]; get rid of temphash1; if existing tashidx then no temphash2
    for(size_t i = 0; i < t1.rows.size(); i++) {
        if(tempHash2.find(t1.rows[i][t1_col]) != tempHash2.end()) {
            if(!quietMode) {
                for(size_t x = 0; x < tempHash2[t1.rows[i][t1_col]].size(); x++) {
                    for(size_t j = 0; j < colsToPrint.size(); j++) {
                        if(!colsToPrint[j].first) {
                            std::cout << t2.rows[tempHash2[t1.rows[i][t1_col]][x]][colsToPrint[j].second] << " ";
                        }
                        else {
                            std::cout << t1.rows[i][colsToPrint[j].second] << " ";
                        }
                    }
                    count++;
                    std::cout << '\n';
                }
                
            }
            else {
                count += tempHash2[t1.rows[i][t1_col]].size();
            }
        }
    }
    
    std::cout << "Printed " << count << " rows from joining " << table1 << " to " << table2 << '\n';
    
    tempHash2.clear();
    // tempHash1.clear();
}

void Database::execute() {
    
    std::string cmd;
    
    // std::cout << "% ";
    std::cin >> cmd;

    while (cmd != "QUIT") {
        // do some stuff
        if (std::cin.fail()) {
            std::cerr << "Error: Reading from cin has failed\n";
            exit(1);
        } // if

        if (cmd == "CREATE") {
            // create
            std::cout << "% ";
            std::string tableName;
            std::cin >> tableName;
            if (tables.find(tableName) != tables.end()) {
                std::cout << "Error during CREATE: Cannot create already " 
                          << "existing table " << tableName << "\n";
                std::string trash;
                getline(std::cin, trash);
                std::cin >> cmd;
                continue;
            }
            create(tableName);
            std::cin >> cmd;
            continue;
        }
        else if (cmd[0] == '#') {
            // comment
            std::cout << "% ";
            std::string trash;
            getline(std::cin, trash);
            std::cin >> cmd;
            continue;
        }
        else if (cmd == "REMOVE") {
            // remove
            std::cout << "% ";
            std::string tableName;
            std::cin >> tableName;
            if (tables.find(tableName) == tables.end()) {
                std::cout << "Error during REMOVE: " << tableName
                          << " does not name a table in the database\n";
                std::string trash;
                getline(std::cin, trash);
                std::cin >> cmd;
                continue;
            }
            remove(tableName);
            std::cin >> cmd;
            continue;
        }
        else if (cmd == "INSERT") {
            // insert
            std::cout << "% ";
            std::string trash;
            std::cin >> trash;
            std::string tableName;
            std::cin >> tableName;
            if (tables.find(tableName) == tables.end()) {
                std::cout << "Error during INSERT: " << tableName
                          << " does not name a table in the database\n";
                getline(std::cin, trash);
                std::cin >> cmd;
                continue;
            }
            tables[tableName].insert(tableName);
            std::cin >> cmd;
            continue;
        }
        else if (cmd == "DELETE") {
            std::cout << "% ";
            std::string trash;
            std::cin >> trash;
            std::string tableName;
            std::cin >> tableName;

            if (tables.find(tableName) == tables.end()) {
                std::cout << "Error during DELETE: " << tableName
                          << " does not name a table in the database\n";
                getline(std::cin, trash);
                std::cin >> cmd;
                continue;
            }

            tables[tableName].deleteRow(tableName);
            std::cin >> cmd;
            continue;
        }
        else if (cmd == "PRINT") {
            std::cout << "% ";
            std::string trash;
            std::cin >> trash;
            std::string tableName;
            std::cin >> tableName;

            if (tables.find(tableName) == tables.end()) {
                std::cout << "Error during PRINT: " << tableName
                          << " does not name a table in the database\n";
                getline(std::cin, trash);
                std::cin >> cmd;
                continue;
            }

            tables[tableName].print(tableName, quietMode);
            std::cin >> cmd;
            continue;
        }
        else if (cmd == "JOIN") {
            // join
            std::cout << "% ";
            
            std::string tableName1;
            std::cin >> tableName1;
            std::string trash;
            std::cin >> trash;
            std::string tableName2;
            std::cin >> tableName2;
            if (tables.find(tableName1) == tables.end()) {
                std::cout << "Error during JOIN: " << tableName1
                          << " does not name a table in the database\n";
                getline(std::cin, trash);
                std::cin >> cmd;
                continue;
            }
            if (tables.find(tableName2) == tables.end()) {
                std::cout << "Error during JOIN: " << tableName2
                          << " does not name a table in the database\n";
                getline(std::cin, trash);
                std::cin >> cmd;
                continue;
            }
            join(tableName1, tableName2, quietMode);
            std::cin >> cmd;
            continue;
        }
        else if (cmd == "GENERATE") {
            // generate
            std::cout << "% ";
            std::string trash;
            std::cin >> trash;
            std::string tableName;
            std::cin >> tableName;

            if (tables.find(tableName) == tables.end()) {
                std::cout << "Error during GENERATE: " << tableName
                          << " does not name a table in the database\n";
                getline(std::cin, trash);
                std::cin >> cmd;
                continue;
            }

            tables[tableName].generate(tableName);
            std::cin >> cmd;
            continue;
        }
        else {
            std::string trash;
            getline(std::cin, trash);
            std::cout << "% " << "Error: unrecognized command\n";
            std::cin >> cmd;
            continue;
            // std::cerr << "Error: Unknown command line option\n";
            // exit(1);
            // std::cout << "% ";
            // std::cin >> cmd;
            // continue;
        }
    }
    std::cout << "% Thanks for being silly!" << '\n';
}