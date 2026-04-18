export function parseFlapMeta(meta) { // [ADDED]

  if (!meta) {
    return {
      description: null,
      imageUrl: null,
      websiteUrl: null,
      twitterUrl: null,
      telegramUrl: null
    };
  }

  let imageUrl = null;

  if (meta.image) {

    if (meta.image.startsWith("ipfs://")) {

      imageUrl = meta.image.replace(
        "ipfs://",
        "https://ipfs.io/ipfs/"
      );

    } else if (meta.image.startsWith("baf")) {

      imageUrl = `https://ipfs.io/ipfs/${meta.image}`;

    } else {

      imageUrl = meta.image;

    }

  }

  return {

    description: meta.description || null,

    imageUrl,

    websiteUrl: meta.website || null,

    twitterUrl: meta.twitter || null,

    telegramUrl: meta.telegram || null

  };
}