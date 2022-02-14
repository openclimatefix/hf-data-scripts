from subprocess import Popen

for year in range(2015,2022):
    for month in range(13):
        for day in range(32):
            path = f"https://mtarchive.geol.iastate.edu/{year}/{str(month).zfill(2)}/{str(day).zfill(2)}/mrms/ncep/GaugeCorr_QPE_01H/"
            args = ['wget', '-r', '-nc', '--no-parent', path]
            process =Popen(args)
            process.wait()
            path = f"https://mtarchive.geol.iastate.edu/{year}/{str(month).zfill(2)}/{str(day).zfill(2)}/mrms/ncep/PrecipFlag/"
            args = ['wget', '-r', '-nc', '--no-parent', path]
            process =Popen(args)
            process.wait()
            path = f"https://mtarchive.geol.iastate.edu/{year}/{str(month).zfill(2)}/{str(day).zfill(2)}/mrms/ncep/PrecipRate/"
            args = ['wget', '-r', '-nc', '--no-parent', path]
            process =Popen(args)
            process.wait()
