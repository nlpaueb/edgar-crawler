'''
Description: This file contains hard-coded lists of items for 10-K, 8-K, and an dictionary for 10-Q filings.
In the case of 10-Q filings, the items are divided into two parts: part_1 and part_2. 
'''


item_list_10k = [
    "1",
    "1A",
    "1B",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "7A",
    "8",
    "9",
    "9A",
    "9B",
    "9C",
    "10",
    "11",
    "12",
    "13",
    "14",
    "15",
    "16",
    "SIGNATURE",
]

item_list_8k = [
    "1.01",
    "1.02",
    "1.03",
    "1.04",
    "2.01",
    "2.02",
    "2.03",
    "2.04",
    "2.05",
    "2.06",
    "3.01",
    "3.02",
    "3.03",
    "4.01",
    "4.02",
    "5.01",
    "5.02",
    "5.03",
    "5.04",
    "5.05",
    "5.06",
    "5.07",
    "5.08",
    "6.01",
    "6.02",
    "6.03",
    "6.04",
    "6.05",
    "7.01",
    "8.01",
    "9.01",
    "SIGNATURE",
]

item_list_8k_obsolete = [
    "1",
    "2",
    "3",
    "4",
    "5",
    "6",
    "7",
    "8",
    "9",
    "10",
    "11",
    "12",
    "SIGNATURE",
]

item_list_10q = {
    "part_1": [
        "1",
        "2",
        "3",
        "4",
    ],
    "part_2": [
        "1",
        "1A",
        "2",
        "3",
        "4",
        "5",
        "6",
        "SIGNATURE",
    ]
}