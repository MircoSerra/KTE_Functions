import src.KTE_artesian.KTE_artesian_actual as ktaa

df = ktaa.get_artesian_actual_data([100238993, 100238991], '2022-06-29', '2022-07-08',
                                   ganularity='h')
print('ok')
