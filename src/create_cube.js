const zipObject = require("lodash").zipObject;

/**
 * returns a pipeline to create a cube over the given dimensions and measures.
 *
 * @param {Array<String>} dimensions  all dimensions to group by
 * @param {Array<String>} measures  all measures to aggregate over
 */
function createCube(dimensions, measures, outCollection) {
  // replace dots with underscores
  const safeDimensions = dimensions.map((d) => d.replace(".", "_"));
  const safeMeasures = measures.map((m) => m.replace(".", "_"));

  const mapMeasure = (measure) => {
    const sumField = `${measure}_sum`;
    const minField = `${measure}_min`;
    const maxField = `${measure}_max`;
    const countField = `${measure}_count`;

    return {
      [sumField]: { $sum: `$${measure}` },
      [minField]: { $min: `$${measure}` },
      [maxField]: { $max: `$${measure}` },
      [countField]: {
        $sum: { $cond: { if: { $ifNull: [`$${measure}`, false] }, then: 1, else: 0 } },
      },
    };
  };

  const mappedMeasures = safeMeasures.reduce((acc, val) => {
    return { ...acc, ...mapMeasure(val) };
  }, {});

  const nestedMeasure = (measure) => {
    const sumField = `${measure}_sum`;
    const minField = `${measure}_min`;
    const maxField = `${measure}_max`;
    const countField = `${measure}_count`;

    return {
      [measure]: {
        sum: sumField,
        min: minField,
        max: maxField,
        count: countField,
      },
    };
  };

  const nestedMeasures = safeMeasures.reduce((acc, val) => {
    return { ...acc, ...nestedMeasure(val) };
  }, {});

  const unwrappedDimensions = safeDimensions.reduce((acc, val) => {
    return { ...acc, [val]: `$_id.${val}` };
  }, {});

  return [
    {
      $group: {
        _id: safeDimensions.reduce((acc, val) => {
          return { ...acc, [val]: `$${val}` };
        }, {}),
        count: { $sum: 1 },
        ...mappedMeasures,
      },
    },
    {
      $project: {
        _id: 0,
        count: 1,
        ...nestedMeasures,
        ...unwrappedDimensions,
      },
    },
    { $merge: outCollection },
  ];
}



module.exports = createCube;
