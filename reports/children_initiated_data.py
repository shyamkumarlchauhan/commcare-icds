from collections import OrderedDict, defaultdict
from datetime import datetime

from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, MONTHLY

from django.db.models.aggregates import Sum
from django.utils.translation import ugettext as _

from custom.icds_reports.const import LocationTypes
from custom.icds_reports.models import AggChildHealthMonthly
from custom.icds_reports.utils import apply_exclude

RED = '#de2d26'
ORANGE = '#fc9272'
BLUE = '#006fdf'
PINK = '#fee0d2'
GREY = '#9D9D9D'


def get_children_initiated_data_map(domain, config, loc_level, show_test=False):

    def get_data_for(filters):
        filters['month'] = datetime(*filters['month'])
        queryset = AggChildHealthMonthly.objects.filter(
            **filters
        ).values(
            '%s_name' % loc_level
        ).annotate(
            in_month=Sum('cf_initiation_in_month'),
            eligible=Sum('cf_initiation_eligible'),
        )

        if not show_test:
            queryset = apply_exclude(domain, queryset)
        return queryset

    map_data = {}

    in_month_total = 0
    valid_total = 0

    for row in get_data_for(config):
        valid = row['eligible']
        name = row['%s_name' % loc_level]

        in_month = row['in_month']

        in_month_total += (in_month or 0)
        valid_total += (valid or 0)

        value = (in_month or 0) * 100 / (valid or 1)
        row_values = {
            'children': in_month or 0,
            'all': valid or 0
        }
        if value < 20:
            row_values.update({'fillKey': '0%-20%'})
        elif 20 <= value < 60:
            row_values.update({'fillKey': '20%-60%'})
        elif value >= 60:
            row_values.update({'fillKey': '60%-100%'})

        map_data.update({name: row_values})

    fills = OrderedDict()
    fills.update({'0%-20%': RED})
    fills.update({'20%-60%': ORANGE})
    fills.update({'60%-100%': PINK})
    fills.update({'defaultFill': GREY})

    return [
        {
            "slug": "severe",
            "label": "Percent Children (6-8 months) initiated Complementary Feeding",
            "fills": fills,
            "rightLegend": {
                "average": (in_month_total * 100) / float(valid_total or 1),
                "info": _((
                    "Percentage of children between 6 - 8 months given timely introduction to solid, "
                    "semi-solid or soft food."
                )),
                "last_modify": datetime.utcnow().strftime("%d/%m/%Y"),
            },
            "data": map_data,
        }
    ]


def get_children_initiated_data_chart(domain, config, loc_level, show_test=False):
    month = datetime(*config['month'])
    three_before = datetime(*config['month']) - relativedelta(months=3)

    config['month__range'] = (three_before, month)
    del config['month']

    chart_data = AggChildHealthMonthly.objects.filter(
        **config
    ).values(
        'month', '%s_name' % loc_level
    ).annotate(
        in_month=Sum('cf_initiation_in_month'),
        eligible=Sum('cf_initiation_eligible'),
    ).order_by('month')

    if not show_test:
        chart_data = apply_exclude(domain, chart_data)

    data = {
        'blue': OrderedDict()
    }

    dates = [dt for dt in rrule(MONTHLY, dtstart=three_before, until=month)]

    for date in dates:
        miliseconds = int(date.strftime("%s")) * 1000
        data['blue'][miliseconds] = {'y': 0, 'all': 0, 'in_month': 0}

    best_worst = {}
    for row in chart_data:
        date = row['month']
        in_month = row['in_month']
        location = row['%s_name' % loc_level]
        valid = row['eligible']

        if location in best_worst:
            best_worst[location].append(in_month / (valid or 1))
        else:
            best_worst[location] = [in_month / (valid or 1)]

        date_in_miliseconds = int(date.strftime("%s")) * 1000

        data_for_month = data['blue'][date_in_miliseconds]
        data_for_month['in_month'] += in_month
        data_for_month['all'] += valid
        data_for_month['y'] = data_for_month['in_month'] / float(data_for_month['all'] or 1)

    top_locations = sorted(
        [dict(loc_name=key, percent=sum(value) / len(value)) for key, value in best_worst.iteritems()],
        key=lambda x: x['percent'],
        reverse=True
    )

    return {
        "chart_data": [
            {
                "values": [
                    {
                        'x': key,
                        'y': value['y'],
                        'all': value['all'],
                        'in_month': value['in_month']
                    } for key, value in data['blue'].iteritems()
                ],
                "key": "% Children began complementary feeding",
                "strokeWidth": 2,
                "classed": "dashed",
                "color": BLUE
            }
        ],
        "all_locations": top_locations,
        "top_three": top_locations[:5],
        "bottom_three": top_locations[-5:],
        "location_type": loc_level.title() if loc_level != LocationTypes.SUPERVISOR else 'State'
    }


def get_children_initiated_sector_data(domain, config, loc_level, show_test=False):
    group_by = ['%s_name' % loc_level]
    if loc_level == LocationTypes.SUPERVISOR:
        config['aggregation_level'] += 1
        group_by.append('%s_name' % LocationTypes.AWC)

    config['month'] = datetime(*config['month'])
    data = AggChildHealthMonthly.objects.filter(
        **config
    ).values(
        *group_by
    ).annotate(
        in_month=Sum('cf_initiation_in_month'),
        eligible=Sum('cf_initiation_eligible'),
    ).order_by('%s_name' % loc_level)

    if not show_test:
        data = apply_exclude(domain, data)

    loc_data = {
        'green': 0,
        'orange': 0,
        'red': 0
    }
    tmp_name = ''
    rows_for_location = 0

    chart_data = {
        'green': [],
        'orange': [],
        'red': []
    }

    tooltips_data = defaultdict(lambda: {
        'children': 0,
        'all': 0
    })

    for row in data:
        valid = row['eligible']
        name = row['%s_name' % loc_level]

        if tmp_name and name != tmp_name:
            chart_data['green'].append([tmp_name, (loc_data['green'] / float(rows_for_location or 1))])
            chart_data['orange'].append([tmp_name, (loc_data['orange'] / float(rows_for_location or 1))])
            chart_data['red'].append([tmp_name, (loc_data['red'] / float(rows_for_location or 1))])
            rows_for_location = 0
            loc_data = {
                'green': 0,
                'orange': 0,
                'red': 0
            }
        in_month = row['in_month']
        row_values = {
            'children': in_month or 0,
            'all': valid or 0
        }
        for prop, value in row_values.iteritems():
            tooltips_data[name][prop] += value

        value = (in_month or 0) * 100 / float(valid or 1)

        if value < 20.0:
            loc_data['red'] += 1
        elif 20.0 <= value < 60.0:
            loc_data['orange'] += 1
        elif value >= 60.0:
            loc_data['green'] += 1

        tmp_name = name
        rows_for_location += 1

    chart_data['green'].append([tmp_name, (loc_data['green'] / float(rows_for_location or 1))])
    chart_data['orange'].append([tmp_name, (loc_data['orange'] / float(rows_for_location or 1))])
    chart_data['red'].append([tmp_name, (loc_data['red'] / float(rows_for_location or 1))])

    return {
        "tooltips_data": tooltips_data,
        "chart_data": [
            {
                "values": chart_data['green'],
                "key": "0%-20%",
                "strokeWidth": 2,
                "classed": "dashed",
                "color": RED
            },
            {
                "values": chart_data['orange'],
                "key": "20%-60%",
                "strokeWidth": 2,
                "classed": "dashed",
                "color": ORANGE
            },
            {
                "values": chart_data['red'],
                "key": "60%-100%",
                "strokeWidth": 2,
                "classed": "dashed",
                "color": PINK
            }
        ]
    }
