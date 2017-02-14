# def _save_cache(folder, lastmodif_hash):
#     fpath = join(folder, '.folder_hash')
#     with open(fpath, 'w') as f:
#         f.write(lastmodif_hash)
#
# def _get_file_list(folder):
#     file_list = []
#     for (dir_, _, files) in os.walk(folder):
#         if dir_ == folder:
#             continue
#         for f in files:
#             fpath = join(dir_, f)
#             if fpath.endswith('pkl') and os.path.basename(fpath) != 'all.pkl':
#                 file_list.append(fpath)
#     return file_list
#
# def _merge(file_list):
#     pbar = report.ProgressBar(len(file_list))
#     out = dict()
#     for (i, fpath) in enumerate(file_list):
#         d = unpickle(fpath)
#         if isinstance(d, collections.Iterable):
#             out.update(d)
#         else:
#             key = os.path.basename(fpath).split('.')[0]
#             out[int(key)] = d
#         pbar.update(i+1)
#     pbar.finish()
#     return out
#
# def pickle_merge(folder):
#     """Merges pickle files from the specified folder and save it to `all.pkl`.
#     """
#     file_list = _get_file_list(folder)
#
#     if len(file_list) == 0:
#         print('There is nothing to merge because no file'+
#               ' has been found in %s.' % folder)
#         return
#
#     with report.BeginEnd('Computing hashes'):
#         ha = path.folder_hash(folder, ['all.pkl', '.folder_hash'])
#
#     subfolders = [d for d in os.listdir(folder) if isdir(join(folder, d))]
#
#     with path.temp_folder() as tf:
#         for sf in subfolders:
#             path.make_sure_path_exists(join(tf, sf))
#             path.cp(join(folder, sf), join(tf, sf))
#         file_list = _get_file_list(tf)
#
#         with report.BeginEnd('Merging pickles'):
#             out = _merge(file_list)
#
#     with report.BeginEnd('Storing pickles'):
#         pickle(out, join(folder, 'all.pkl'))
#     _save_cache(folder, ha)
#
#     return out
