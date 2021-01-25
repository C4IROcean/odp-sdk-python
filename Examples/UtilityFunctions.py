import seaborn as sns
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import cmocean
import cartopy
import cartopy.crs as ccrs
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter
import cartopy.feature as cfeature
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
from matplotlib.lines import Line2D
import scipy.interpolate 
import metpy.interpolate


def interpolate_casts_to_z(df, variable, z_int, max_z_extrapolation=3, max_z_copy_single_value=1, kind='linear'):
    """Interpolate profiles in dataframe to prescribed depth level.

    Takes a complete dataframe from ODP and interpolates each cast by filtering out the values from each unique cast

    Args:
        df: Pandas DataFrame fromODP
        variable: Variable name to be interpolated as in the dataframe (Temperature, Oxygen, etc)
        z_int: List of the desired depth intervals to return, i.e [0,10,20]
        max_z_extrapolation: The maximum length to allow extrapolating. Nan values outside this distance.
        max_z_copy_single_value: If only one row is present in the cast, this is the maximum distance between the point and the interpolation level for copying the value
        kind: Type of interpolation as in interpolate_profile

    Returns:
        DataFrame of parameter values at prescribed depth levels.

    .. py::currentmodule:: Examples
    """

    nz = len(z_int)
    data = np.zeros((len(df.externalId.unique())*nz, 6), 'O')

    for i, ext_id in enumerate(df.externalId.unique()):
        _df = df[df.externalId == ext_id]
        v_int = interpolate_profile(_df[['z', variable]].values, z_int, max_z_extrapolation,
                                    max_z_copy_single_value, kind)

        data[i * nz:(i + 1)*nz, 0:4] = (ext_id, _df.datetime.iloc[0], _df.lon.iloc[0], _df.lat.iloc[0])
        data[i * nz:(i + 1)*nz, 4] = z_int
        data[i * nz:(i + 1)*nz, 5] = v_int

    return pd.DataFrame(
        data, columns=['externalId', 'datetime', 'lon', 'lat', 'z', variable]
    ).astype({'lon': float, 'lat': float, variable: float, 'z': float})


def interpolate_to_grid(points, values, int_points, interp_type='linear', minimum_neighbors=3,
                        gamma=0.25, kappa_star=5.052, search_radius=0.1, rbf_func='linear', rbf_smooth=0.001, rescale=True):
    """Interpolate unstructured ND data to a Nd grid

    Powered by the metpy library

    Args:
        points: (N,D) array of points, typically latitude and longitude
        values: (N,1) array of corresponding values, i.e Temperature, Oxygen etc
        int_points:  list of arrays for gridding i.e lat/long grid --> (np.linspace(-25,35,60*10+1),np.linspace(50,80,30*10+1))
        interp_type:  What type of interpolation to use. Available options include:
            1) "linear", "nearest", "cubic", or "rbf" from `scipy.interpolate`.
            2) "natural_neighbor", "barnes", or "cressman" from `metpy.interpolate`.
            Default "linear".
        minimum_neighbors: Minimum number of neighbors needed to perform barnes or cressman interpolation for a point.
            Default is 3.
        gamma:
            Adjustable smoothing parameter for the barnes interpolation. Default 0.25.
        kappa_star: Response parameter for barnes interpolation, specified nondimensionally in terms of the Nyquist.
            Default 5.052
        search_radius: A search radius to use for the barnes and cressman interpolation schemes.
            If search_radius is not specified, it will default to the average spacing of observations.
        rbf_func: Specifies which function to use for Rbf interpolation.
            Options include: 'multiquadric', 'inverse', 'gaussian', 'linear', 'cubic',
            'quintic', and 'thin_plate'. Defualt 'linear'. See `scipy.interpolate.Rbf` for more information.
        rbf_smooth:
            Smoothing value applied to rbf interpolation.  Higher values result in more smoothing.
        rescale:

    Returns:
        values_interpolated: Array representing the interpolated values for each input point
    """

    if rescale:
        norm_mean = points.mean(axis=0)
        points -= norm_mean

        norm_max = points.max(axis=0)
        points /= norm_max

        for i in range(len(int_points)):
            int_points[i] -= norm_mean[i]
            int_points[i] /= norm_max[i]

    grid_points = np.meshgrid(*int_points)
    grid_points_ravel = [c.ravel() for c in grid_points]
    grid_points_ravel = np.array(grid_points_ravel).T
    
    if interp_type != 'rbf':
        grid_values = metpy.interpolate.interpolate_to_points(
            points, values, grid_points_ravel, interp_type=interp_type, minimum_neighbors=minimum_neighbors,
            gamma=gamma, kappa_star=kappa_star, search_radius=search_radius, rbf_func=rbf_func, rbf_smooth=rbf_smooth
        ).reshape(grid_points[0].shape)
    else:
        
        points_transposed = np.array(points).transpose()
        xi_transposed = np.array(grid_points_ravel).transpose()
        rbfi = scipy.interpolate.Rbf(*points_transposed, values, function=rbf_func, smooth=rbf_smooth)
        grid_values = rbfi(*xi_transposed).reshape(grid_points[0].shape)

    if rescale == True:
        for i in range(len(int_points)):
            int_points[i] *= norm_max[i]
            int_points[i] += norm_mean[i]
        grid_points = np.meshgrid(*int_points)

    return grid_points, grid_values


def interpolate_profile(zv, z_int, max_z_extrapolation=10, max_z_copy_single_value=1, kind='linear'):
    """Interpolate profile zv (depth, parameter) to a user defined depth.

    Args:
        zv: 2-D array of depth and a parameter (temperature, oxygen, ...)
        z_int: 1-D array of depth levles to interpolate to
        max_z_extrapolation: Maximum distance to extrapolate outside profile. Use 0 for no extrapolation.
        max_z_copy_single_value: Maximum distance for copying the value of a single value profile.
        kind: Specifies the kind of interpolation as a string
            (‘linear’, ‘nearest’, ‘zero’, ‘slinear’, ‘quadratic’, ‘cubic’, ‘previous’, ‘next’, where ‘zero’, ‘slinear’)

    Returns:
        Returns array of interpolated values

    Example::

        zv=array(
           [[ 0.        , 21.64599991],
           [ 9.93530941, 21.54500008],
           [19.87013626, 20.96299934],
           [20.40699959, 29.80448341],
           [19.36800003, 49.67173004],
           [18.8010006 , 74.50308228],
           [18.27400017, 99.3314209 ]]
        )

        z_int = [0,0,25,50,75,100,125]

        v_int = interpolate_profile(ZV,z_int)

        print(v_int)
        # >>> array([21.64599991, 20.67589412, 19.36050431, 18.79045314, 18.25980907, nan])

    """

    # Sorting by depth
    zv = zv[np.argsort(zv[:, 0])]
    z_int = np.sort(z_int)

    n_zv = len(zv)
    v_int = np.ones(len(z_int)) * np.nan

    if n_zv > 1:
        # Interpolation for more than one row
        # Remove interpolations point outside extrapolation range

        ind = (z_int >= (zv[0, 0] - max_z_extrapolation)) & (z_int <= (zv[-1, 0] + max_z_extrapolation))
        _z_int = z_int[ind]

        f = scipy.interpolate.interp1d(
            zv[:, 0], zv[:, 1],
            kind='linear',
            axis=-1,
            copy=True,
            bounds_error=None,
            fill_value='extrapolate',
            assume_sorted=True
        )

        v_int[ind] = f(_z_int)

    elif n_zv == 1:
        # Only one row of data, copy value if within distance (max_z_copy_single_value)
        dist = z_int-zv[0, 0]
        v_int[dist <= max_z_copy_single_value] = zv[0, 1]

    return v_int


def plot_casts(variable, df, longitude, latitude, cmap='viridis', vrange=[None, None]):
    """Plot casts
    Args:
        variable: str of oceanographic vairable, i.e. 'Temperature'
        df: Pandas DataFrame from ODP with lat, lon, and variable columns
        longitude: List of min and max longitude, i.e [-10,35]
        latitude: List of min and max latitude, i.e [50,80]
        cmap: colormap specification
        vrange: Ranges for variables to be showsn, i.e. [0,20]
    Returns:
        Map with variable measurments plotted as points
    """
    extent = [longitude[0], longitude[1],latitude[0], latitude[1]]
    central_lon = np.mean(extent[:2])
    central_lat = np.mean(extent[2:])
    
    fig = plt.figure(figsize=(12, 14))
    ax = plt.axes(projection=ccrs.Mercator(central_lon))
    geo_map(ax)
    
    cs = plt.scatter(df.lon, df.lat, c=df[variable], cmap=cmap, vmin=vrange[0], vmax=vrange[1],s=10, transform = ccrs.PlateCarree())
    cb = fig.colorbar(cs, ax=ax, orientation='horizontal', ticklocation='auto', pad=0.05)
    cb.ax.set_title('{} ({})'.format(variable, get_units()[variable]), fontsize=14)

    
def plot_grid(longitude, latitude, int_lon, int_lat, g, cmap='viridis', vrange=[None, None],
              crs_latlon=ccrs.PlateCarree(), variable_name=''):
    """Plot Grid
    Args:
        int_lon: (M,N) array of longitude grid
        int_lat: (M,N) array of latitude grid
        g: (M,N) grid to be shown
        cmap: colormap
        vrange: Ranges for grid to be shown i.e [0,35]
        crs_latlon:
        variable_name:
    Returns:
        Map with interpolated values
    """

    fig = plt.figure(figsize=(12, 14))
    extent = [longitude[0], longitude[1],latitude[0], latitude[1]]
    central_lon = np.mean(extent[:2])
    central_lat = np.mean(extent[2:])
    
    ax = plt.axes(projection=ccrs.Mercator(central_lon))

    geo_map(ax)
    cf = plt.contourf(int_lon, int_lat, g, 100,
                      transform=ccrs.PlateCarree(),
                      cmap=cmap)
    cb = fig.colorbar(cf, ax=ax, orientation='horizontal', ticklocation='auto', pad=0.05)
    cb.ax.set_title(variable_name)


def get_units():
    """Get dict describing the units of the different columns

    Returns:
        Dict of units
    """

    return {
        'z': 'm',
        'Oxygen': 'umol/kg',
        'Temperature': 'degree_C',
        'Chlorophyll': 'ugram/l',
        'Pressure': 'dbar',
        'Nitrate': 'umol/kg',
        'Latitude': 'degrees_north',
        'Longitude': 'degrees_east'
    }


def geo_map(ax):
    """Helper function for mapping
    Args:
        ax: Matplotlib axis
    """
    # add land and coastline
    ax.add_feature(cfeature.LAND, facecolor='#F9F6F1', zorder=1, edgecolor='#1B3278')
    ax.add_feature(cfeature.COASTLINE, color='#1B3278', linewidth=0.5, zorder=1)
    ax.add_feature(cfeature.BORDERS, color='#1B3278',linewidth=0.4, zorder=1)
    ax.add_feature(cfeature.OCEAN, facecolor='#D2E0F5')
    ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True, linewidth=1, color='gray', alpha=0.7, linestyle=':')

def plot_nulls(df, var_list=None):
    """Plot percentage of nulls for each variable in variable list.

    Takes a dataframe from ODP and a list of variables and plots the percentage of missing values

    Args:
        df: Pandas dataframe from ODP
        var_list: list of variables (column names) that user is interested in default list is all the columns

    Returns:
        Plot of percentage of values missing at each measuremtn (lat, lon, depth)
    """

    if var_list:
        if isinstance(var_list, list) == False:
            var_list = [var_list]
        if all(elem in df.columns for elem in var_list):
            info = df[var_list]
        else:
            print('Variable not in dataframe')
    else:
        info = df
    
    info_nulls = info.isnull().sum(axis = 0).sort_values()

    # Variables we want to plot
    variables = info_nulls.index

    # Percentage missing
    perct = np.round(info_nulls.values / len(info), decimals=4) * 100

    plt.figure(figsize=(10, 8))

    ax = sns.barplot(
        y=variables,
        x=perct,
        color='cornflowerblue'
    )
    ax.set_title('Percentage of Values Missing (Total Rows: {})'.format(len(df)), fontsize=20)
    ax.set_ylabel('Variables', size=15)
    ax.set_xlabel('Percent Missing', size=15)
    ax.tick_params(axis='both', which='major', labelsize=12)

    # Add values next to bar
    for p in ax.patches:
        _x = p.get_x() + p.get_width() + 0.03
        _y = p.get_y() + p.get_height() - 0.3
        value = p.get_width().round(3)
        ax.text(_x, _y, str(value) + '%', ha="left", fontsize=10);

    return ax


def missing_values(df, var_list):
    """Get dataframe of nulls for each variable in variable list.

    Takes a dataframe from ODP and a list of variables and return dataframe of missing values

    Args:
        df: Pandas DataFrame from ODP
        var_list: list of variables (column names) that user is interested in default list is all the columns

    Returns:
        Dataframe percentage of values missing at each measuremtn (lat, lon, depth)
    """

    if var_list:
        if not isinstance(var_list, list):
            var_list = [var_list]
        if all(elem in df.columns for elem in var_list):
            info = df[var_list]
        else:
            print('Variable not in dataframe')
    else:
        info = df

    info_nulls = info.isnull().sum(axis=0).sort_values()

    # Variables we want to plot
    variables = info_nulls.index

    # Percentage missing
    perct = np.round(info_nulls.values/len(info), decimals=4)*100

    return pd.DataFrame({
        'Variables': variables,
        'Null Values': info_nulls.values,
        'Percentage Missing': perct
    })


def plot_meta_stats(df, variable):
    """Get bar graph of percentage of data belonging to a specific variable subset in the metadata

    Args:
        df: Pandas DataFrame with `extId`-column
        variable: Variable in subset of metadata

    Returns:
        Bar graph with percentage of data belonging to variable subset (i.e. data belonging to different modes of data collection ('dataset'))
    """

    colors = sns.color_palette('bright') + sns.color_palette('deep') + sns.color_palette('Set3')
    df2 = df[[variable, 'extId']].groupby(variable).count().sort_values(by='extId', ascending=False).reset_index(0)

    plt.figure(figsize=(10, 6))
    ax = sns.barplot(
        y=df2[variable],
        x=df2['extId'] / df2['extId'].sum() * 100,
        palette=colors
    )

    ax.set_ylabel(variable, size=15)
    ax.set_xlabel('Percentage of casts', size=15)
    ax.tick_params(axis='both', which='major', labelsize=12)

    for p in ax.patches:

        _x = p.get_x() + p.get_width()
        _y = p.get_y() + p.get_height() - 0.3
        value = p.get_width().round(3)

        ax.text(_x, _y, str(value) + '%', ha="left", fontsize=10)


def plot_distributions(df, var_list):
    """Plot the distributions of the values for a list of variables

    Args:
        df: Pandas DataFrame from ODP containing oceanographic variables and values
        var_list: list of variables (column names) that should be plotted

    Returns:
        Plots of distributions of values for each variable in variable list
    """

    if not isinstance(var_list, list):
        var_list = [var_list]
    
    for variable in var_list:
        plt.figure(figsize=(10, 6))

        ax = sns.distplot(df[variable], kde=False)
        ax.set_ylabel('Count', size=15)
        ax.set_xlabel(variable + ' value', size=15)
        ax.tick_params(axis='both', which='major', labelsize=12)


def plot_datasets(df, variable, latitude, longitude):
    """Plots on a map casts belonging to specific dataset (mode of data collection, i.e. ctd, xbt)

    Args:
        df: Pandas DataFrame
        variable: Variable of choice
        latitude: Bounding box latitude
        longitude: Bounding box longitude

    Returns:
        Map with color coded casts based on dataset_code
    """

    variable_list = list(
        df[[variable, 'extId']].groupby(variable)
                               .count()
                               .reset_index(0)
                               .sort_values(by='extId', ascending=False)[variable]
    )

    colors = sns.color_palette('bright') + sns.color_palette('deep') + sns.color_palette('Set3')
    color_plot = colors[0:len(df[variable].unique())]

    fig = plt.figure(figsize=(12, 12))
    extent = [longitude[0], longitude[1],latitude[0], latitude[1]]
    central_lon = np.mean(extent[:2])
    central_lat = np.mean(extent[2:])
    ax = fig.add_subplot(1, 1, 1, projection=ccrs.Mercator(central_lon))

    legend_elements = []
    for i, j in enumerate(variable_list):
        sns.scatterplot(
            x="lon",
            y="lat",
            data=df[df[variable] == j],
            color=color_plot[i],
            s=30,
            marker='o',
            edgecolor='white',
            linewidths=0.05, transform = ccrs.PlateCarree()
        )
        legend_elements.append(Line2D([0], [0], color=colors[i], lw=4, label=variable_list[i]))


    ax.legend(handles=legend_elements, loc='lower center',
              ncol=4, borderaxespad=-10., prop={'size': 12})
    geo_map(ax), plot_meta_stats(df, variable)

