#spatialspark
## System Overview
<center style="border:solid 1px #ccc;">
<img src="http://cnzhujie.bj.bcebos.com/github/spatial/overview.png?responseContentDisposition=attachment"/>
<br/>图1. System Overview
</center>
##Filters
<table style="width:100%;">
    <thead>
        <tr>
            <th>类名</th>
            <th>简称</th>
            <th>解释</th>
            <th>过滤（构造）参数</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>OTimeTrajectoryFilter</td>
            <td>OTime</td>
            <td>通过开始时间进行过滤</td>
            <td>
                1. filter.OTime.time 时间戳格式(s)<br>
                2. filter.OTime.relation 可选值：eq（等于）,lt（小于）,gt（大于）,let（小于等于）,get（大于等于）
            </td>
        </tr>
        <tr>
            <td>DTimeTrajectoryFilter</td>
            <td>DTime</td>
            <td>通过结束时间进行过滤</td>
            <td>
                1. filter.DTime.time 时间戳格式(s)<br>
                2. filter.DTime.relation 可选值：eq（等于）,lt（小于）,gt（大于）,let（小于等于）,get（大于等于）
            </td>
        </tr>
        <tr>
            <td>TravelTimeTrajectoryFilter</td>
            <td>TravelTime</td>
            <td>通过轨迹运行总时间进行过滤</td>
            <td>
                1. filter.TravelTime.time 时间(s)<br>
                2. filter.TravelTime.relation 可选值：eq（等于）,lt（小于）,gt（大于）,let（小于等于）,get（大于等于）
            </td>
        </tr>
        <tr>
            <td>OPointTrajectoryFilter</td>
            <td>OPoint</td>
            <td>通过起点范围进行过滤</td>
            <td>
                1. filter.OPoint.minLat 最小的纬度范围<br/>
                2. filter.OPoint.maxLat 最大的纬度范围<br/>
                3. filter.OPoint.minLng 最小的经度范围<br/>
                4. filter.OPoint.maxLng 最大的经度范围
            </td>
        </tr>
        <tr>
            <td>DPointTrajectoryFilter</td>
            <td>DPoint</td>
            <td>通过起点范围进行过滤</td>
            <td>
                1. filter.DPoint.minLat 最小的纬度范围<br/>
                2. filter.DPoint.maxLat 最大的纬度范围<br/>
                3. filter.DPoint.minLng 最小的经度范围<br/>
                4. filter.DPoint.maxLng 最大的经度范围
            </td>
        </tr>
        <tr>
            <td>TravelDistanceTrajectoryFilter</td>
            <td>TravelDistance</td>
            <td>通过轨迹运行总距离进行过滤</td>
            <td>
                1. filter.TravelDistance.dis 距离(m)<br>
                2. filter.TravelDistance.relation 可选值：eq（等于）,lt（小于）,gt（大于）,let（小于等于）,get（大于等于）
            </td>
        </tr>
        <tr>
            <td>AvgSpeedTrajectoryFilter</td>
            <td>AvgSpeed</td>
            <td>通过轨迹的全局平均速度进行过滤</td>
            <td>
                1. filter.AvgSpeed.speed 速度(m/s)<br>
                2. filter.AvgSpeed.relation 可选值：eq（等于）,lt（小于）,gt（大于）,let（小于等于）,get（大于等于）
            </td>
        </tr>
        <tr>
            <td>AvgSimpleTimeTrajectoryFilter</td>
            <td>AvgSimpleTime</td>
            <td>通过轨迹的全局平均采样频率（时间）进行过滤</td>
            <td>
                1. filter.AvgSimpleTime.time 采样间隔时间(s)<br>
                2. filter.AvgSimpleTime.relation 可选值：eq（等于）,lt（小于）,gt（大于）,let（小于等于）,get（大于等于）
            </td>
        </tr>
        <tr>
            <td>PassRangeTrajectoryFilter</td>
            <td>PassRange</td>
            <td>通过判断是否穿过某个区域范围进行过滤</td>
            <td>
                1. filter.PassRange.minLat 最小的纬度范围<br/>
                2. filter.PassRange.maxLat 最大的纬度范围<br/>
                3. filter.PassRange.minLng 最小的经度范围<br/>
                4. filter.PassRange.maxLng 最大的经度范围
            </td>
        </tr>
    </tbody>
</table>

##Features
<table style="width:100%;">
    <thead>
        <tr>
            <th>类名</th>
            <th>简称</th>
            <th>解释</th>
            <th>过滤（构造）参数</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>SimpleSpeedGPSPointFilter</td>
            <td>SimpleSpeed</td>
            <td>通过轨迹采样点的平均采样速度进行过滤（轨迹上的每个采样点都有一个对应的速度，求这些速度的平均值）</td>
            <td>
                1. filter.SimpleSpeed.speed 速度(m/s)<br>
                2. filter.SimpleSpeed.relation 可选值：eq（等于）,lt（小于）,gt（大于）,let（小于等于）,get（大于等于）
            </td>
        </tr>
        <tr>
            <td>RangeGPSPointFilter</td>
            <td>Range</td>
            <td>通过区域空间范围进行过滤，不在这个范围的轨迹（采样点）都舍弃</td>
            <td>
                1. filter.Range.minLat 最小的纬度范围<br/>
                2. filter.Range.maxLat 最大的纬度范围<br/>
                3. filter.Range.minLng 最小的经度范围<br/>
                4. filter.Range.maxLng 最大的经度范围
            </td>
        </tr>
    </tbody>
</table>
