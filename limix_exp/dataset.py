import h5py
import numpy as np
from os.path import join
from numpy import asarray
import config
import limix_util as lu

class Dataset(object):
    """docstring for Dataset"""
    def __init__(self, workspace_id, dataset_id):
        self.dataset_id = dataset_id
        self.workspace_id = workspace_id
        self._f = None

    def _setup(self):
        if self._f is None:
            fpath = join(config.root_dir(), self.workspace_id, 'dataset',
                         self.dataset_id + '.hdf5')
            self._f = h5py.File(fpath, 'r')

    def __exit__(self, type_, value, traceback):
        self.close()

    def close(self):
        if self._f is not None:
            self._f.close()

class H2Dataset(Dataset):
    """docstring for H2Dataset"""
    def __init__(self, workspace_id, dataset_id):
        super(H2Dataset, self).__init__(workspace_id, dataset_id)

    def get_K_on_the_fly(self, samples, chrom):
        self._setup()
        with lu.BeginEnd('K SNPs extraction'):
            X = self._f['genotype/%s/SNP' % chrom][samples,:]
        return lu.data_.kinship_estimation(X)

    def get_K(self):
        self._setup()
        return self._f['genotype/K']

    @property
    def nchroms(self):
        self._setup()
        names = self._f['genotype'].keys()
        return sum([1 for n in names if 'chrom' in n])

    def chrom(self, c):
        self._setup()
        return self._f['genotype/chrom%02d/SNP' % c]

    @property
    def nsnps(self):
        nsnps = 0
        for i in xrange(1, self.nchroms+1):
            nsnps += self.chrom(i).shape[1]
        return nsnps

    @property
    def nsamples(self):
        self._setup()
        return self._f['genotype/chrom10/SNP'].shape[0]

    def get_z(self, chrom):
        self._setup()
        z = self._f['genotype/%s/z' % chrom][:]
        return (z - z.mean()) / z.std()

class H2QuarterDataset(Dataset):
    """docstring for H2Dataset"""
    def __init__(self, workspace_id, dataset_id):
        super(H2QuarterDataset, self).__init__(workspace_id, dataset_id)

    def get_K_on_the_fly(self, samples, chrom):
        self._setup()
        with lu.BeginEnd('K SNPs extraction'):
            X = self._f['genotype/%s/SNP' % chrom]
            nsnps = int(X.shape[1]/4)
            X = self._f['genotype/%s/SNP' % chrom][samples,:nsnps]
        return lu.data_.kinship_estimation(X)

    def get_K(self):
        self._setup()
        return self._f['genotype/K']

    @property
    def nchroms(self):
        self._setup()
        names = self._f['genotype'].keys()
        return sum([1 for n in names if 'chrom' in n])

    def chrom(self, c):
        self._setup()
        return self._f['genotype/chrom%02d/SNP' % c]

    @property
    def nsnps(self):
        nsnps = 0
        for i in xrange(1, self.nchroms+1):
            nsnps += self.chrom(i).shape[1]
        return nsnps

    @property
    def nsamples(self):
        self._setup()
        return self._f['genotype/chrom10/SNP'].shape[0]

    def get_z(self, chrom):
        self._setup()
        z = self._f['genotype/%s/z' % chrom][:]
        return (z - z.mean()) / z.std()

class QTLCisDataset(Dataset):
    """docstring for QTLCisDataset"""
    def __init__(self, workspace_id, dataset_id):
        super(QTLCisDataset, self).__init__(workspace_id, dataset_id)

    def get_K(self):
        K = self._f['/genotype/K'].value
        return asarray(K, float)

    def get_X(self, name):
        X = self._f['phenotype'][name]['X'].value
        return asarray(X, float)

    def get_marker_pos(self, name):
        pos = self._f['phenotype'][name]['pos'].value
        return asarray(pos, int)

    def get_phenotype_ids(self):
        pheno_ids = self._f['phenotype'].keys()
        return asarray([str(pi) for pi in pheno_ids])

    def get_ntrials(self, name):
        ntrials = self._f['phenotype'][name]['ntrials'].value
        return asarray(ntrials, float)

    def get_suc(self, name):
        suc = self._f['phenotype'][name]['suc'].value
        return asarray(suc, float)

    def get_gene_function(self, name):
        return str(self._f['phenotype'][name]['gene_func'].value)

class QTLDataset(Dataset):
    """docstring for QTLDataset"""
    def __init__(self, workspace_id, dataset_id):
        super(QTLDataset, self).__init__(workspace_id, dataset_id)

    def get_LOCO_K(self, chrom_id):
        Kall = self.get_Kall()
        K = self.get_K(chrom_id)
        return asarray(Kall - K, float)

    def get_Kall(self):
        K = self._f['/genotype/K'].value
        return asarray(K, float)

    def get_K(self, chrom_id):
        K = self._f['/genotype/%s/K' % chrom_id].value
        return asarray(K, float)

    def get_X(self, chrom_id):
        X = self._f['genotype'][chrom_id]['X'].value
        return asarray(X, float)

    def get_y(self):
        y = self._f['phenotype']['y'].value
        return asarray(y, float)

    def get_marker_pos(self, chrom_id):
        pos = self._f['genotype'][chrom_id]['pos'].value
        return asarray(pos, int)

class HeavyQTLDataset(Dataset):
    """docstring for HeavyQTLDataset"""
    def __init__(self, workspace_id, dataset_id):
        super(HeavyQTLDataset, self).__init__(workspace_id, dataset_id)

    # def get_LOCO_K(self, chrom_id):
    #     Kall = self.get_Kall()
    #     K = self.get_K(chrom_id)
    #     return asarray(Kall - K, float)

    def get_Kall(self):
        self._setup()
        K = self._f['/genotype/K'].value
        return asarray(K, float)

    # def get_K(self, chrom_id):
    #     K = self._f['/genotype/%s/K' % chrom_id].value
    #     return asarray(K, float)

    def get_X(self, chrom_id):
        self._setup()
        return self._f['genotype'][chrom_id]['X']

    def get_y(self):
        self._setup()
        y = self._f['phenotype']['y'].value
        return asarray(y, float)

    def get_marker_pos(self, chrom_id):
        self._setup()
        pos = self._f['genotype'][chrom_id]['pos'].value
        return asarray(pos, int)

    def get_K_on_the_fly(self, samples, chrom):
        self._setup()
        with lu.time_.Timer():
            X = self._f['genotype/%s/SNP' % chrom][samples,:]
        return lu.data_.kinship_estimation(X)

    def get_K(self):
        return self._f['genotype/K']

    @property
    def nchroms(self):
        self._setup()
        names = self._f['genotype'].keys()
        return sum([1 for n in names if 'chrom' in n])

    def chrom(self, chromid):
        self._setup()
        return self._f['genotype/%s/SNP' % chromid]

    def nsnps(self, chromid):
        return self.chrom(chromid).shape[1]

    @property
    def nsamples(self):
        self._setup()
        return self._f['genotype/chrom10/SNP'].shape[0]

    def get_z(self, chrom):
        self._setup()
        z = self._f['genotype/%s/z' % chrom][:]
        return (z - z.mean()) / z.std()

    def get_pos(self, chrom):
        self._setup()
        pos = self._f['genotype/%s/pos' % chrom][:]
        return asarray(pos, float)

    def get_mean(self, chrom):
        self._setup()
        pos = self._f['genotype/%s/mean' % chrom][:]
        return asarray(pos, float)

    def get_std(self, chrom):
        self._setup()
        pos = self._f['genotype/%s/std' % chrom][:]
        return asarray(pos, float)

class HeavyQuarterQTLDataset(Dataset):
    """docstring for HeavyQTLDataset"""
    def __init__(self, workspace_id, dataset_id):
        super(HeavyQuarterQTLDataset, self).__init__(workspace_id, dataset_id)

    def get_Kall(self):
        self._setup()
        K = self._f['/genotype/K'].value
        return asarray(K, float)

    def get_X(self, chrom_id):
        self._setup()
        return self._f['genotype'][chrom_id]['X']

    def get_y(self):
        self._setup()
        y = self._f['phenotype']['y'].value
        return asarray(y, float)

    def get_marker_pos(self, chrom_id):
        self._setup()
        pos = self._f['genotype'][chrom_id]['pos'].value
        return asarray(pos, int)

    def get_K_on_the_fly(self, samples, chrom):
        self._setup()
        X_ = self._f['genotype/%s/SNP' % chrom]
        nsnps = int(X_.shape[1]/4)
        X = np.empty((len(samples), nsnps))
        X_.read_direct(X, np.s_[samples,:nsnps], np.s_[:])
        # X = self._f['genotype/%s/SNP' % chrom][samples,:]
        return lu.data_.kinship_estimation(X)

    def get_K(self):
        return self._f['genotype/K']

    @property
    def nchroms(self):
        self._setup()
        names = self._f['genotype'].keys()
        return sum([1 for n in names if 'chrom' in n])

    def chrom(self, chromid):
        self._setup()
        return self._f['genotype/%s/SNP' % chromid]

    def nsnps(self, chromid):
        return self.chrom(chromid).shape[1]

    @property
    def nsamples(self):
        self._setup()
        return self._f['genotype/chrom10/SNP'].shape[0]

    def get_z(self, chrom):
        self._setup()
        z = self._f['genotype/%s/z' % chrom][:]
        return (z - z.mean()) / z.std()

    def get_pos(self, chrom):
        self._setup()
        pos = self._f['genotype/%s/pos' % chrom][:]
        return asarray(pos, float)

    def get_mean(self, chrom):
        self._setup()
        pos = self._f['genotype/%s/mean' % chrom][:]
        return asarray(pos, float)

    def get_std(self, chrom):
        self._setup()
        pos = self._f['genotype/%s/std' % chrom][:]
        return asarray(pos, float)

class Heavy20QTLDataset(Dataset):
    """docstring for Heavy20QTLDataset"""
    def __init__(self, workspace_id, dataset_id):
        super(Heavy20QTLDataset, self).__init__(workspace_id, dataset_id)

    def get_Kall(self):
        self._setup()
        K = self._f['/genotype/K'].value
        return asarray(K, float)

    def get_X(self, chrom_id):
        self._setup()
        return self._f['genotype'][chrom_id]['X']

    def get_y(self):
        self._setup()
        y = self._f['phenotype']['y'].value
        return asarray(y, float)

    def get_marker_pos(self, chrom_id):
        self._setup()
        pos = self._f['genotype'][chrom_id]['pos'].value
        return asarray(pos, int)

    def get_K_on_the_fly(self, samples, chrom):
        self._setup()
        X_ = self._f['genotype/%s/SNP' % chrom]
        nsnps = X_.shape[1]
        X = np.empty((len(samples), nsnps))
        X_.read_direct(X, np.s_[samples,:nsnps], np.s_[:])
        return lu.data_.kinship_estimation(X)

    def get_K(self):
        return self._f['genotype/K']

    @property
    def nchroms(self):
        self._setup()
        names = self._f['genotype'].keys()
        return sum([1 for n in names if 'chrom' in n])

    def chrom(self, chromid):
        self._setup()
        return self._f['genotype/%s/SNP' % chromid]

    def nsnps(self, chromid):
        return self.chrom(chromid).shape[1]

    @property
    def nsamples(self):
        self._setup()
        return self._f['genotype/chrom10/SNP'].shape[0]

    def get_z(self, chrom):
        self._setup()
        z = self._f['genotype/%s/z' % chrom][:]
        return (z - z.mean()) / z.std()

    def get_pos(self, chrom):
        self._setup()
        pos = self._f['genotype/%s/pos' % chrom][:]
        return asarray(pos, float)

    def get_mean(self, chrom):
        self._setup()
        pos = self._f['genotype/%s/mean' % chrom][:]
        return asarray(pos, float)

    def get_std(self, chrom):
        self._setup()
        pos = self._f['genotype/%s/std' % chrom][:]
        return asarray(pos, float)


def load_dataset(workspace_id, dataset_id):
    fpath = join(config.root_dir(), workspace_id, dataset_id + '.hdf5')
    with h5py.File(fpath, 'r') as f:
        dataset_cls = locals()[f['dataset_cls'].value]
        return dataset_cls(workspace_id, dataset_id)
