# local, remote
table_dist = {
    'mini_01': [([], ['lineitem'])],
    'mini_02': [([], ['orders'])],
    'mini_03': [
         (['orders', 'lineitem'], []),
                ([], ['orders', 'lineitem']),
                 (['lineitem'], ['orders'])
        ],
    'mini_04': [],
    'mini_05': [
                ([], ['lineitem']),
                (['lineitem'], [])],
    'mini_06': [
        (['part', 'nation'], ['partsupp', 'supplier', 'region']),
                (['part'], ['nation', 'partsupp', 'supplier', 'region']),
                (['partsupp', 'supplier'], ['part', 'nation', 'region']),
                ([], ['part', 'nation', 'partsupp', 'supplier', 'region'])
    ],
    'q_02_v1': [
        (['part', 'nation'], ['partsupp', 'supplier', 'region']),
        (['part'], ['nation', 'partsupp', 'supplier', 'region']),
        (['partsupp', 'supplier'], ['part', 'nation', 'region']),
        (['partsupp', 'supplier', 'region'], ['part', 'nation'])
    ],
    'q_04_v1': [
        (['customer'], ['orders', 'lineitem']),
        (['customer', 'orders'], ['lineitem']),
        (['lineitem', 'orders'], ['customer'])
    ],
    'q_05_v1':[
        (['orders', 'supplier', 'region'], ['customer', 'nation', 'lineitem']),
        (['lineitem', 'orders', 'region'], ['customer', 'nation', 'supplier']),
        (['customer', 'nation', 'lineitem', 'supplier', 'region'], ['orders']),
        (['nation', 'supplier'], ['customer', 'region', 'orders', 'lineitem'])
    ],
    'q_10_v1': [(['orders'], ['customer', 'nation', 'lineitem']),
                (['lineitem', 'orders'], ['customer', 'nation']),
                (['customer', 'nation', 'lineitem'], ['orders'])
                ],
    'q_13_v1': [
                (['customer'], ['orders']),
                (['orders'], ['customer']),
                # ([], ['orders', 'customer']),
                # (['orders', 'customer'], [])
    ],
    'q_03_v1': [
                (['orders'], ['lineitem', 'customer']),
                 (['lineitem'], ['orders', 'customer'])
    ],
    'q_14_v1': [
                (['lineitem'], ['part']),
                (['part'], ['lineitem']),
                # ([], ['part', 'lineitem'])
    ],
    'q_15_v1': [
        (['part'], ['lineitem']),
        (['lineitem'], ['part']),
        # ([], ['part', 'lineitem'])
    ],
    'q_17_v1': [
        (['part'], ['lineitem']),
        (['lineitem'], ['part']),
        # ([], ['part', 'lineitem'])
    ],

    'q_18_v1': [
        (['customer'], ['lineitem', 'orders']),
        (['lineitem', 'orders'], ['customer'])
    ],

    'random_01': [
        (['supplier'], ['lineitem', 'partsupp']),
    ]
}

date_columns = {
    'orders': [4],
    'lineitem': [10, 11, 12]
}

db_config = {
    'host': 'localhost',
    'port': 5432,
    'db': 'sf01',
    'schema': 'public',
    'user': 'postgres',
    'passwd': 'postgres'
}

# db_config = {
#     'host': 'localhost',
#     'port': 6000,
#     'db': 'sf01',
#     'schema': 'sf01',
#     'user': 'sf01',
#     'passwd': 'sf01'
# }

large_rs = [2986, 2981, ]

local_data_path = '../datasets/'
# local_data_path = '../datasets/sf04/'