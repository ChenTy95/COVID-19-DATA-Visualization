var dataType = 1;
var dataTypeWorld = 1;
var dataTypeStr = new Array("", "累计确诊", "现存确诊", "累计治愈", "累计死亡");

function getFormatDate(dateValue, splitChar) {
    if (dateValue == '')
        var date = new Date();
    else
        var date = new Date(dateValue);
    var year = date.getFullYear();
    var month = date.getMonth() + 1;
    if (month < 10) month = '0' + month;
    var day = date.getDate();
    if (day < 10) day = '0' + day;

    return year + splitChar + month + splitChar + day;
}

function createDateArray(length) {
    var dateArray = new Array(length);
    dateArray[0] = '2020-01-22';
    var date = new Date('2020-01-22');
    for (var i = 1; i < length; i++) {
        date.setDate(date.getDate() + 1);
        dateArray[i] = getFormatDate(date, '-');
    }
    return dateArray;
}

function getMaxValue(array) {
    var minValue = 0;
    array.forEach(element => {
        if (element["value"] > minValue) minValue = element["value"];
    });
    return minValue;
}

function get2ndMaxValue(array) {
    var maxValue = getMaxValue(array);
    var result = 0;
    array.forEach(element => {
        if (element["value"] > result && element["value"] < maxValue)
            result = element["value"];
    });
    return result;
}

function setMapChina(dataType) {
    var jsonChina = "";
    var jsonChinaDateArr;
    $.ajax({
        method: "GET",
        url: "http://localhost:8684/COVID-19-DATA-WS.asmx/GetChinaProvinceTimeSeriesData?date=" + getFormatDate('', '-') + "&dataType=" + dataType,
        dataType: "json",
        success: function (json) {
            jsonChina = json;
            jsonChinaDateArr = createDateArray(json.length);
            chartMapChina.setOption({
                baseOption: {
                    timeline: {
                        data: jsonChinaDateArr,
                        currentIndex: json.length - 1
                    },
                    title: {
                        text: '全国 COVID-19 疫情数据 - ' + dataTypeStr[dataType],
                        subtext: getFormatDate('', '-')
                    },
                    visualMap: {
                        max: getMaxValue(json[json.length - 1]["series"][0]["data"])
                    }
                },
                options: json
            });
        }
    });
    // 地图点击事件监听
    chartMapChina.on('click', function (param) {
        setLineProvince('', param.data['name']);
        setBarCity('', param.data['name']);
    });

    // 时间轴切换时事件监听
    chartMapChina.on('timelinechanged', function (timeLineIndex) {
        chartMapChina.setOption({
            baseOption: {
                title: {
                    subtext: jsonChinaDateArr[timeLineIndex.currentIndex]
                },
                visualMap: {
                    max: getMaxValue(jsonChina[timeLineIndex.currentIndex]["series"][0]["data"])
                }
            }
        });
    });
}

function setLineProvince(date, provinceName) {
    $.ajax({
        method: "GET",
        url: "http://localhost:8684/COVID-19-DATA-WS.asmx/GetEachProvinceTimeSeriesData?date=" + getFormatDate(date, '-') + "&dataType=0&provinceName=" + provinceName,
        dataType: "json",
        success: function (json) {
            chartLineProvince.setOption({
                title: {
                    text: provinceName + " COVID-19 疫情数据"
                },
                xAxis: [{
                    data: createDateArray(json[0]["data"].length)
                }],
                series: json
            });
        }
    });
}

function switchData(dataTypeSel) {
    dataType = dataTypeSel;
    setMapChina(dataTypeSel);
}

function setLineChina(date) {
    $.ajax({
        method: "GET",
        url: "http://localhost:8684/COVID-19-DATA-WS.asmx/GetChinaTimeSeriesData?date=" + getFormatDate(date, '-') + "&dataType=0",
        dataType: "json",
        success: function (json) {
            chartLineChina.setOption({
                xAxis: [{
                    data: createDateArray(json[0]["data"].length)
                }],
                series: json
            });
        }
    });
}

function setBarCity(date, provinceName) {
    $.ajax({
        method: "GET",
        url: "http://localhost:8684/COVID-19-DATA-WS.asmx/GetEachProvinceDetailDateData?provinceName=" + provinceName + "&date=" + getFormatDate(date, '-') + "&dataType=0",
        dataType: "json",
        success: function (json) {
            chartBarCity.setOption({
                title: {
                    text: provinceName + '各地区最新 COVID-19 疫情数据'
                },
                xAxis: {
                    data: json[0]
                },
                series: json[1]
            });
        }
    });
}

function setMapWorld(dataType) {
    var jsonWorld = "";
    var jsonWorldDateArr;
    $.ajax({
        method: "GET",
        url: "http://localhost:8684/COVID-19-DATA-WS.asmx/GetWorldCountryTimeSeriesData?date=" + getFormatDate('', '-') + "&dataType=" + dataType,
        dataType: "json",
        success: function (json) {
            jsonWorld = json;
            jsonWorldDateArr = createDateArray(json.length);
            chartMapWorld.setOption({
                baseOption: {
                    timeline: {
                        data: jsonWorldDateArr,
                        currentIndex: json.length - 1
                    },
                    title: {
                        text: '全球 COVID-19 疫情数据 - ' + dataTypeStr[dataType],
                        subtext: getFormatDate('', '-')
                    },
                    visualMap: {
                        max: getMaxValue(json[json.length - 1]["series"][0]["data"])
                    }
                },
                options: json
            });
        }
    });
    // 地图点击事件监听
    chartMapWorld.on('click', function (param) {
        setLineProvince('', param.data['name']);
        setBarCity('', param.data['name']);
    });

    // 时间轴切换时事件监听
    chartMapWorld.on('timelinechanged', function (timeLineIndex) {
        chartMapWorld.setOption({
            baseOption: {
                title: {
                    subtext: jsonWorldDateArr[timeLineIndex.currentIndex]
                },
                visualMap: {
                    max: getMaxValue(jsonWorld[timeLineIndex.currentIndex]["series"][0]["data"])
                }
            }
        });
    });
}

function switchWorldData(dataTypeSel) {
    dataTypeWorld = dataTypeSel;
    setMapWorld(dataTypeSel);
}

function setPieWorld(date) {
    $.ajax({
        method: "GET",
        url: "http://localhost:8684/COVID-19-DATA-WS.asmx/GetWorldLead10CountryData?date=" + getFormatDate(date, '-'),
        dataType: "json",
        success: function (json) {
            chartPieWorld.setOption({
                legend: {
                    data: json[0]
                },
                series: {
                    data: json[1]
                }
            });
        }
    });
}

function setLineWorld(date) {
    $.ajax({
        method: "GET",
        url: "http://localhost:8684/COVID-19-DATA-WS.asmx/GetWorldTimeSeriesData?date=" + getFormatDate(date, '-') + "&dataType=0",
        dataType: "json",
        success: function (json) {
            chartLineWorld.setOption({
                xAxis: [{
                    data: createDateArray(json[0]["data"].length)
                }],
                series: json
            });
        }
    });
}