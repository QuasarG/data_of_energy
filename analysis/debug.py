import xarray as xr
ds = xr.Dataset({"var": (("x"), [1, 2, 3])})
ds.to_netcdf(r"E:\PythonProjiects\Data_of_energy_competition\test.nc")